// Copyright Redis(https://github.com/redis/redis)
// Copyright Kvrocks Core Team (bugfix and adapted to kvrocks coding style)
// All rights reserved.
//
#include "scripting.h"

#include <string>

#include "util.h"
#include "sha1.h"
#include "server.h"
#include "redis_connection.h"
#include "redis_cmd.h"

/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to doule to string */
#define MAX_LONG_DOUBLE_CHARS 5*1024

extern "C" {
LUALIB_API int (luaopen_cjson)(lua_State *L);
LUALIB_API int (luaopen_struct)(lua_State *L);
LUALIB_API int (luaopen_cmsgpack)(lua_State *L);
LUALIB_API int (luaopen_bit)(lua_State *L);
}

namespace Lua {
  lua_State* CreateState() {
    lua_State *lua = lua_open();
    loadLibraries(lua);
    removeUnsupportedFunctions(lua);
    loadFuncs(lua);
    return lua;
  }

  void loadFuncs(lua_State *lua) {
    lua_newtable(lua);

    /* redis.call */
    lua_pushstring(lua,  "call");
    lua_pushcfunction(lua, redisCallCommand);
    lua_settable(lua,  -3);

    /* redis.pcall */
    lua_pushstring(lua,  "pcall");
    lua_pushcfunction(lua, redisPCallCommand);
    lua_settable(lua,  -3);

    /* redis.sha1hex */
    lua_pushstring(lua, "sha1hex");
    lua_pushcfunction(lua, redisSha1hexCommand);
    lua_settable(lua, -3);

    /* redis.error_reply and redis.status_reply */
    lua_pushstring(lua, "error_reply");
    lua_pushcfunction(lua, redisErrorReplyCommand);
    lua_settable(lua, -3);
    lua_pushstring(lua, "status_reply");
    lua_pushcfunction(lua, redisStatusReplyCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua, "redis");

    /* Replace math.random and math.randomseed with our implementations. */
    lua_getglobal(lua, "math");
    lua_setglobal(lua,  "math");

  /* Add a helper function we use for pcall error reporting.
  * Note that when the error is in the C function we want to report the
  * information about the caller, that's what makes sense from the point
  * of view of the user debugging a script. */
    {
      const char *err_func = "local dbg = debug\n"
                        "function __redis__err__handler(err)\n"
                        "  local i = dbg.getinfo(2,'nSl')\n"
                        "  if i and i.what == 'C' then\n"
                        "    i = dbg.getinfo(3,'nSl')\n"
                        "  end\n"
                        "  if i then\n"
                        "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
                        "  else\n"
                        "    return err\n"
                        "  end\n"
                        "end\n";
      luaL_loadbuffer(lua, err_func, strlen(err_func), "@err_handler_def");
      lua_pcall(lua, 0, 0, 0);
    }
  }

  Status evalGenericCommand(Redis::Connection *conn,
                            const std::vector<std::string> &args,
                            bool evalsha,
                            std::string *output) {
    int64_t numkeys = 0;
    char funcname[43];
    lua_State *lua = conn->GetServer()->Lua();

    auto s = Util::StringToNum(args[2], &numkeys);
    if (!s.IsOK()) {
      return s;
    }
    if (numkeys > int64_t(args.size()-3)) {
      return Status(Status::NotOK, "Number of keys can't be greater than number of args");
    } else if (numkeys < -1) {
      return Status(Status::NotOK, "Number of keys can't be negative");
    }

    /* We obtain the script SHA1, then check if this function is already
     * defined into the Lua state */
    funcname[0] = 'f';
    funcname[1] = '_';
    if (!evalsha) {
      SHA1Hex(funcname+2, args[1].c_str(), args[1].size());
    }

    /* Push the pcall error handler function on the stack. */
    lua_getglobal(lua, "__redis__err__handler");

    /* Try to lookup the Lua function */
    lua_getglobal(lua, funcname);
    if (lua_isnil(lua, -1)) {
      lua_pop(lua, 1); /* remove the nil from the stack */
      /* Function not defined... let's define it if we have the
       * body of the function. If this is an EVALSHA call we can just
       * return an error. */
      if (evalsha) {
        lua_pop(lua, 1); /* remove the error handler from the stack. */
        return Status(Status::NotOK, "NOSCRIPT No matching script. Please use EVAL");
      }
      std::string sha;
      s = createFunction(lua, args[1], &sha);
      if (!s.IsOK()) {
        lua_pop(lua, 1); /* remove the error handler from the stack. */
        /* The error is sent to the client by luaCreateFunction()
         * itself when it returns NULL. */
        return s;
      }
      /* Now the following is guaranteed to return non nil */
      lua_getglobal(lua, funcname);
    }
    /* Populate the argv and keys table accordingly to the arguments that
     * EVAL received. */
    setGlobalArray(lua, "KEYS", std::vector<std::string>(args.begin()+3, args.begin()+3+numkeys));
    setGlobalArray(lua, "ARGV", std::vector<std::string>(args.begin()+3+numkeys, args.end()));
    int err = lua_pcall(lua, 0, 1, -2);
    if (err) {
      std::string msg = std::string("Error running script (call to ") + funcname + "): "+ lua_tostring(lua, -1);
      *output = Redis::Error(msg);
      lua_pop(lua, 2);
    } else {
      *output = replyToRedisReply(lua);
      lua_pop(lua, 1);
    }

    /* Call the Lua garbage collector from time to time to avoid a
    * full cycle performed by Lua, which adds too latency.
    *
    * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
    * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
    * for every command uses too much CPU. */
    #define LUA_GC_CYCLE_PERIOD 50
    {
      static int64_t gc_count = 0;

      gc_count++;
      if (gc_count == LUA_GC_CYCLE_PERIOD) {
        lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
        gc_count = 0;
      }
    }
    return Status::OK();
  }

  int redisCallCommand(lua_State *lua) {
    return redisGenericCommand(lua, 1);
  }

  int redisPCallCommand(lua_State *lua) {
    return redisGenericCommand(lua, 0);
  }
  int redisGenericCommand(lua_State *lua, int raise_error) {
    int j, argc = lua_gettop(lua);
    std::vector<std::string> args;

    if (argc == 0) {
      pushError(lua, "Please specify at least one argument for redis.call()");
      return raise_error ? raiseError(lua) : 1;
    }
    for (j = 0; j < argc; j++) {
      char dbuf[64];
      if (lua_type(lua, j+1) == LUA_TNUMBER) {
        lua_Number num = lua_tonumber(lua, j+1);
        snprintf(dbuf, sizeof(dbuf), "%.17g", static_cast<double>(num));
        args.emplace_back(dbuf);
      } else {
        const char *str = lua_tostring(lua, j+1);
        if (str == nullptr) break; /* no a string */
        args.emplace_back(str);
      }
    }
    if (j != argc) {
      pushError(lua, "Lua redis() command arguments must be strings or integers");
      return raise_error ? raiseError(lua) : 1;
    }

    auto commands = Redis::GetCommands();
    auto cmd_iter = commands->find(Util::ToLower(args[0]));
    if (cmd_iter == commands->end()) {
      pushError(lua, "Unknown Redis command called from Lua script");
      return raise_error ? raiseError(lua) : 1;
    }
    auto redisCmd = cmd_iter->second;
    auto cmd = redisCmd->factory();
    cmd->SetAttributes(redisCmd);
    cmd->SetArgs(args);
    int arity = cmd->GetAttributes()->arity;
    if (((arity > 0 && argc != arity) || (arity < 0 && argc < -arity))) {
      pushError(lua, "Wrong number of args calling Redis command From Lua script");
      return raise_error ? raiseError(lua) : 1;
    }
    if (cmd->GetAttributes()->flags & Redis::kCmdNoScript) {
      pushError(lua, "This Redis command is not allowed from scripts");
      return raise_error ? raiseError(lua) : 1;
    }

    std::string output;
    Server *srv = GetServer();
    auto s = cmd->Parse(args);
    if (!s.IsOK()) {
      pushError(lua, s.Msg().data());
      return raise_error ? raiseError(lua) : 1;
    }
    s = cmd->Execute(GetServer(), srv->GetCurrentConnection(), &output);
    if (!s.IsOK()) {
      pushError(lua, s.Msg().data());
      return raise_error ? raiseError(lua) : 1;
    }
    redisProtocolToLuaType(lua, output.data());
    return 1;
  }

  void removeUnsupportedFunctions(lua_State *lua) {
    lua_pushnil(lua);
    lua_setglobal(lua, "loadfile");
    lua_pushnil(lua);
    lua_setglobal(lua, "dofile");
  }

  void loadLibraries(lua_State *lua) {
    auto loadLib = [] (lua_State *lua, const char *libname, lua_CFunction func) {
      lua_pushcfunction(lua, func);
      lua_pushstring(lua, libname);
      lua_call(lua,  1, 0);
    };
    loadLib(lua, "", luaopen_base);
    loadLib(lua, LUA_TABLIBNAME, luaopen_table);
    loadLib(lua, LUA_STRLIBNAME, luaopen_string);
    loadLib(lua, LUA_MATHLIBNAME, luaopen_math);
    loadLib(lua, LUA_DBLIBNAME, luaopen_debug);
    loadLib(lua, "cjson", luaopen_cjson);
    loadLib(lua, "struct", luaopen_struct);
    loadLib(lua, "cmsgpack", luaopen_cmsgpack);
    loadLib(lua, "bit", luaopen_bit);
  }

  /* Returns a table with a single field 'field' set to the string value
  * passed as argument. This helper function is handy when returning
  * a Redis Protocol error or status reply from Lua:
  *
  * return redis.error_reply("ERR Some Error")
  * return redis.status_reply("ERR Some Error")
  */
  int redisReturnSingleFieldTable(lua_State *lua, const char *field) {
    if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
      pushError(lua, "wrong number or type of arguments");
      return 1;
    }

    lua_newtable(lua);
    lua_pushstring(lua, field);
    lua_pushvalue(lua, -3);
    lua_settable(lua, -3);
    return 1;
  }

  /* redis.error_reply() */
  int redisErrorReplyCommand(lua_State *lua) {
    return redisReturnSingleFieldTable(lua, "err");
  }

  /* redis.status_reply() */
  int redisStatusReplyCommand(lua_State *lua) {
    return redisReturnSingleFieldTable(lua, "ok");
  }

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
  * function used for sha1ing lua scripts. */
  int redisSha1hexCommand(lua_State *lua) {
    int argc = lua_gettop(lua);
    char digest[41];
    size_t len;
    const char *s;

    if (argc != 1) {
      lua_pushstring(lua, "wrong number of arguments");
      return lua_error(lua);
    }

    s = static_cast<const char *>(lua_tolstring(lua, 1, &len));
    SHA1Hex(digest, s, len);
    lua_pushstring(lua, digest);
    return 1;
  }

/* ---------------------------------------------------------------------------
 * Utility functions.
 * ------------------------------------------------------------------------- */

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of redis.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
void SHA1Hex(char *digest, const char *script, size_t len) {
  SHA1_CTX ctx;
  unsigned char hash[20];
  const char *cset = "0123456789abcdef";
  int j;

  SHA1Init(&ctx);
  SHA1Update(&ctx, (const unsigned char*)script, len);
  SHA1Final(hash, &ctx);

  for (j = 0; j < 20; j++) {
    digest[j*2] = cset[((hash[j]&0xF0)>>4)];
    digest[j*2+1] = cset[(hash[j]&0xF)];
  }
  digest[40] = '\0';
}

/*
 * ---------------------------------------------------------------------------
 * Redis reply to Lua type conversion functions.
 * ------------------------------------------------------------------------- */

/* Take a Redis reply in the Redis protocol format and convert it into a
 * Lua type. Thanks to this function, and the introduction of not connected
 * clients, it is trivial to implement the redis() lua function.
 *
 * Basically we take the arguments, execute the Redis command in the context
 * of a non connected client, then take the generated reply and convert it
 * into a suitable Lua type. With this trick the scripting feature does not
 * need the introduction of a full Redis internals API. The script
 * is like a normal client that bypasses all the slow I/O paths.
 *
 * Note: in this function we do not do any sanity check as the reply is
 * generated by Redis directly. This allows us to go faster.
 *
 * Errors are returned as a table with a single 'err' field set to the
 * error string.
 */

const char * redisProtocolToLuaType(lua_State *lua, const char *reply) {
  const char *p = reply;

  switch (*p) {
    case ':': p = redisProtocolToLuaType_Int(lua, reply); break;
    case '$': p = redisProtocolToLuaType_Bulk(lua, reply); break;
    case '+': p = redisProtocolToLuaType_Status(lua, reply); break;
    case '-': p = redisProtocolToLuaType_Error(lua, reply); break;
    case '*': p = redisProtocolToLuaType_Aggregate(lua, reply, *p); break;
    case '%': p = redisProtocolToLuaType_Aggregate(lua, reply, *p); break;
    case '~': p = redisProtocolToLuaType_Aggregate(lua, reply, *p); break;
    case '_': p = redisProtocolToLuaType_Null(lua, reply); break;
    case '#': p = redisProtocolToLuaType_Bool(lua, reply, p[1]); break;
    case ',': p = redisProtocolToLuaType_Double(lua, reply); break;
  }
  return p;
}

const char *redisProtocolToLuaType_Int(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');
  int64_t value;

  Util::StringToNum(std::string(reply+1, p-reply-1), &value);
  lua_pushnumber(lua, static_cast<lua_Number>(value));
  return p+2;
}

const char *redisProtocolToLuaType_Bulk(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');
  int64_t  bulklen;

  Util::StringToNum(std::string(reply+1, p-reply-1), &bulklen);
  if (bulklen == -1) {
    lua_pushboolean(lua, 0);
    return p+2;
  } else {
    lua_pushlstring(lua, p+2, bulklen);
    return p+2+bulklen+2;
  }
}

const char *redisProtocolToLuaType_Status(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "ok");
  lua_pushlstring(lua, reply+1, p-reply-1);
  lua_settable(lua, -3);
  return p+2;
}

const char *redisProtocolToLuaType_Error(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushlstring(lua, reply+1, p-reply-1);
  lua_settable(lua, -3);
  return p+2;
}

const char *redisProtocolToLuaType_Aggregate(lua_State *lua, const char *reply, int atype) {
  const char *p = strchr(reply+1, '\r');
  int64_t  mbulklen;
  int j = 0;

  Util::StringToNum(std::string(reply+1, p-reply-1), &mbulklen);
  p += 2;
  if (mbulklen == -1) {
    lua_pushboolean(lua, 0);
    return p;
  }
  lua_newtable(lua);
  for (j = 0; j < mbulklen; j++) {
    lua_pushnumber(lua, j+1);
    p = redisProtocolToLuaType(lua, p);
    lua_settable(lua, -3);
  }
  return p;
}

const char *redisProtocolToLuaType_Null(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');
  lua_pushnil(lua);
  return p+2;
}

const char *redisProtocolToLuaType_Bool(lua_State *lua, const char *reply, int tf) {
  const char *p = strchr(reply+1, '\r');
  lua_pushboolean(lua, tf == 't');
  return p+2;
}

const char *redisProtocolToLuaType_Double(lua_State *lua, const char *reply) {
  const char *p = strchr(reply+1, '\r');
  char buf[MAX_LONG_DOUBLE_CHARS+1];
  size_t len = p-reply-1;
  double d;

  if (len <= MAX_LONG_DOUBLE_CHARS) {
    memcpy(buf, reply+1, len);
    buf[len] = '\0';
    d = strtod(buf, nullptr); /* We expect a valid representation. */
  } else {
    d = 0;
  }

  lua_newtable(lua);
  lua_pushstring(lua, "double");
  lua_pushnumber(lua, d);
  lua_settable(lua, -3);
  return p+2;
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by redis.pcall to return errors, which is a lua table
 * with a single "err" field set to the error string. Note that this
 * table is never a valid reply by proper commands, since the returned
 * tables are otherwise always indexed by integers, never by strings. */
void pushError(lua_State *lua, const char *err) {
  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushstring(lua, err);
  lua_settable(lua, -3);
}

std::string replyToRedisReply(lua_State *lua) {
  std::string output;
  int t = lua_type(lua, -1);
  switch (t) {
    case LUA_TSTRING:
      output = Redis::SimpleString(std::string(lua_tostring(lua, -1), lua_strlen(lua, -1)));
      break;
    case LUA_TBOOLEAN:
      output = lua_toboolean(lua, -1) ? Redis::Integer(1) : Redis::NilString();
      break;
    case LUA_TNUMBER:
      output = Redis::Integer((int64_t)(lua_tonumber(lua, -1)));
      break;
    case LUA_TTABLE:
      /* We need to check if it is an array, an error, or a status reply.
       * Error are returned as a single element table with 'err' field.
       * Status replies are returned as single element table with 'ok'
       * field. */

      /* Handle error reply. */
      lua_pushstring(lua, "err");
      lua_gettable(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        output = Redis::Error(lua_tostring(lua, -1));
        lua_pop(lua, 2);
        return output;
      }
      lua_pop(lua, 1); /* Discard field name pushed before. */
      /* Handle status reply. */
      lua_pushstring(lua, "ok");
      lua_gettable(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        output = Redis::SimpleString(lua_tostring(lua, -1));
        lua_pop(lua, 1);
        return output;
      } else {
        int j = 1, mbulklen = 0;
        lua_pop(lua, 1); /* Discard the 'ok' field value we popped */
        while (true) {
          lua_pushnumber(lua, j++);
          lua_gettable(lua, -2);
          t = lua_type(lua, -1);
          if (t == LUA_TNIL) {
            lua_pop(lua, 1);
            break;
          }
          mbulklen++;
          output += replyToRedisReply(lua);
        }
        output = Redis::MultiLen(mbulklen)+output;
      }
      break;
    default:
      output = Redis::NilString();
  }
  lua_pop(lua, 1);
  return output;
}

/* In case the error set into the Lua stack by pushError() was generated
 * by the non-error-trapping version of redis.pcall(), which is redis.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
int raiseError(lua_State *lua) {
  lua_pushstring(lua, "err");
  lua_gettable(lua, -2);
  return lua_error(lua);
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void sortArray(lua_State *lua) {
  /* Initial Stack: array */
  lua_getglobal(lua, "table");
  lua_pushstring(lua, "sort");
  lua_gettable(lua, -2);       /* Stack: array, table, table.sort */
  lua_pushvalue(lua, -3);      /* Stack: array, table, table.sort, array */
  if (lua_pcall(lua, 1, 0, 0)) {
    /* Stack: array, table, error */

    /* We are not interested in the error, we assume that the problem is
     * that there are 'false' elements inside the array, so we try
     * again with a slower function but able to handle this case, that
     * is: table.sort(table, __redis__compare_helper) */
    lua_pop(lua, 1);             /* Stack: array, table */
    lua_pushstring(lua, "sort"); /* Stack: array, table, sort */
    lua_gettable(lua, -2);       /* Stack: array, table, table.sort */
    lua_pushvalue(lua, -3);      /* Stack: array, table, table.sort, array */
    lua_getglobal(lua, "__redis__compare_helper");
    /* Stack: array, table, table.sort, array, __redis__compare_helper */
    lua_call(lua, 2, 0);
  }
  /* Stack: array (sorted), table */
  lua_pop(lua, 1);             /* Stack: array (sorted) */
}

void setGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems) {
  lua_newtable(lua);
  for (size_t i = 0; i < elems.size(); i++) {
    lua_pushlstring(lua, elems[i].c_str(), elems[i].size());
    lua_rawseti(lua, -2, i+1);
  }
  lua_setglobal(lua, var.c_str());
}



/* ---------------------------------------------------------------------------
 * EVAL and SCRIPT commands implementation
 * ------------------------------------------------------------------------- */

/* Define a Lua function with the specified body.
 * The function name will be generated in the following form:
 *
 *   f_<hex sha1 sum>
 *
 * The function increments the reference count of the 'body' object as a
 * side effect of a successful call.
 *
 * On success a pointer to an SDS string representing the function SHA1 of the
 * just added function is returned (and will be valid until the next call
 * to scriptingReset() function), otherwise NULL is returned.
 *
 * The function handles the fact of being called with a script that already
 * exists, and in such a case, it behaves like in the success case.
 *
 * If 'c' is not NULL, on error the client is informed with an appropriate
 * error describing the nature of the problem and the Lua interpreter error. */
Status createFunction(lua_State *lua, const std::string &body, std::string *sha) {
  char funcname[43];

  funcname[0] = 'f';
  funcname[1] = '_';
  SHA1Hex(funcname+2, body.c_str(), body.size());

  std::string funcdef;
  funcdef += "function ";
  funcdef += funcname;
  funcdef += "() ";
  funcdef += body;
  funcdef += "\nend";

  if (luaL_loadbuffer(lua, funcdef.c_str(), funcdef.size(), "@user_script")) {
    std::string errMsg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return Status(Status::NotOK, "Error compiling script (new function): " + errMsg +"\n");
  }
  if (lua_pcall(lua, 0, 0, 0)) {
    std::string errMsg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return Status(Status::NotOK,
                  "Error running script (new function): " + errMsg + "\n");
  }
  *sha = funcdef.substr(2);
  return Status::OK();
}

}  // namespace Lua
