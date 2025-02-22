/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <storage/batch_extractor.h>

#include "command_parser.h"
#include "commander.h"
#include "commands/scan_base.h"
#include "common/io_util.h"
#include "common/rdb_stream.h"
#include "common/string_util.h"
#include "common/time_util.h"
#include "config/config.h"
#include "error_constants.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "stats/disk_stats.h"
#include "storage/rdb/rdb.h"

namespace redis {

class CommandAuth : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto &user_password = args_[1];
    std::string ns;
    AuthResult result = srv->AuthenticateUser(user_password, &ns);
    switch (result) {
      case AuthResult::NO_REQUIRE_PASS:
        return {Status::RedisExecErr, "Client sent AUTH, but no password is set"};
      case AuthResult::INVALID_PASSWORD:
        return {Status::RedisExecErr, "Invalid password"};
      case AuthResult::IS_USER:
        conn->BecomeUser();
        break;
      case AuthResult::IS_ADMIN:
        conn->BecomeAdmin();
        break;
    }
    conn->SetNamespace(ns);
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandNamespace : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    Config *config = srv->GetConfig();
    std::string sub_command = util::ToLower(args_[1]);
    if (config->repl_namespace_enabled && config->IsSlave() && sub_command != "get") {
      return {Status::RedisExecErr, "namespace is read-only for slave"};
    }
    if (args_.size() == 3 && sub_command == "get") {
      if (args_[2] == "*") {
        std::vector<std::string> namespaces;
        auto tokens = srv->GetNamespace()->List();
        for (auto &token : tokens) {
          namespaces.emplace_back(token.second);  // namespace
          namespaces.emplace_back(token.first);   // token
        }
        namespaces.emplace_back(kDefaultNamespace);
        namespaces.emplace_back(config->requirepass);
        *output = ArrayOfBulkStrings(namespaces);
      } else {
        auto token = srv->GetNamespace()->Get(args_[2]);
        if (token.Is<Status::NotFound>()) {
          *output = conn->NilString();
        } else {
          *output = redis::BulkString(token.GetValue());
        }
      }
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = srv->GetNamespace()->Set(args_[2], args_[3]);
      *output = s.IsOK() ? redis::RESP_OK : redis::Error(s);
      LOG(WARNING) << "Updated namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 4 && sub_command == "add") {
      Status s = srv->GetNamespace()->Add(args_[2], args_[3]);
      *output = s.IsOK() ? redis::RESP_OK : redis::Error(s);
      LOG(WARNING) << "New namespace: " << args_[2] << " with token: " << args_[3] << ", addr: " << conn->GetAddr()
                   << ", result: " << s.Msg();
    } else if (args_.size() == 3 && sub_command == "del") {
      Status s = srv->GetNamespace()->Del(args_[2]);
      *output = s.IsOK() ? redis::RESP_OK : redis::Error(s);
      LOG(WARNING) << "Deleted namespace: " << args_[2] << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else {
      return {Status::RedisExecErr, "NAMESPACE subcommand must be one of GET, SET, DEL, ADD"};
    }
    return Status::OK();
  }
};

class CommandKeys : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    const std::string &glob_pattern = args_[1];
    std::vector<std::string> keys;
    redis::Database redis(srv->storage, conn->GetNamespace());

    if (const Status s = util::ValidateGlob(glob_pattern); !s.IsOK()) {
      return {Status::RedisParseErr, "Invalid glob pattern: " + s.Msg()};
    }
    const auto [prefix, suffix_glob] = util::SplitGlob(glob_pattern);
    const rocksdb::Status s = redis.Keys(ctx, prefix, suffix_glob, &keys);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = conn->MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushDB : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (srv->GetConfig()->cluster_enabled) {
      if (srv->slot_migrator->IsMigrationInProgress()) {
        srv->slot_migrator->SetStopMigrationFlag(true);
        LOG(INFO) << "Stop migration task for flushdb";
      }
    }
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.FlushDB(ctx);
    LOG(WARNING) << "DB keys in namespace: " << conn->GetNamespace() << " was flushed, addr: " << conn->GetAddr();
    if (s.ok()) {
      *output = redis::RESP_OK;
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandFlushAll : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (srv->GetConfig()->cluster_enabled) {
      if (srv->slot_migrator->IsMigrationInProgress()) {
        srv->slot_migrator->SetStopMigrationFlag(true);
        LOG(INFO) << "Stop migration task for flushall";
      }
    }

    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.FlushAll(ctx);
    if (s.ok()) {
      LOG(WARNING) << "All DB keys was flushed, addr: " << conn->GetAddr();
      *output = redis::RESP_OK;
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandPing : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    if (args_.size() == 1) {
      *output = redis::SimpleString("PONG");
    } else if (args_.size() == 2) {
      *output = redis::BulkString(args_[1]);
    } else {
      return {Status::NotOK, errWrongNumOfArguments};
    }
    return Status::OK();
  }
};

class CommandSelect : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    Config *config = srv->GetConfig();
    std::string sub_command = util::ToLower(args_[1]);
    if ((sub_command == "rewrite" && args_.size() != 2) || (sub_command == "get" && args_.size() != 3) ||
        (sub_command == "set" && args_.size() != 4)) {
      return {Status::RedisExecErr, errWrongNumOfArguments};
    }

    if (args_.size() == 2 && sub_command == "rewrite") {
      Status s = config->Rewrite(srv->GetNamespace()->List());
      if (!s.IsOK()) return s;

      *output = redis::RESP_OK;
      LOG(INFO) << "# CONFIG REWRITE executed with success";
    } else if (args_.size() == 3 && sub_command == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = conn->MapOfBulkStrings(values);
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->Set(srv, args_[2], args_[3]);
      if (!s.IsOK()) {
        return {Status::RedisExecErr, "CONFIG SET '" + args_[2] + "' error: " + s.Msg()};
      } else {
        *output = redis::RESP_OK;
      }
    } else {
      return {Status::RedisExecErr, "CONFIG subcommand must be one of GET, SET, REWRITE"};
    }
    return Status::OK();
  }
};

class CommandInfo : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<std::string> sections;
    for (size_t i = 1; i < args_.size(); ++i) {
      sections.push_back(args_[i]);
    }
    auto info = srv->GetInfo(conn->GetNamespace(), sections);
    *output = conn->VerbatimString("txt", info);
    return Status::OK();
  }
};

class CommandDisk : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = util::ToLower(args[1]);
    if (opname != "usage") return {Status::RedisInvalidCmd, "Unknown operation"};
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    RedisType type = kRedisNone;
    redis::Disk disk_db(srv->storage, conn->GetNamespace());

    auto s = disk_db.Type(ctx, args_[2], &type);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    uint64_t result = 0;
    s = disk_db.GetKeySize(ctx, args_[2], type, &result);
    if (!s.ok()) {
      // Redis returns the Nil string when the key does not exist
      if (s.IsNotFound()) {
        *output = conn->NilString();
        return Status::OK();
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(result);
    return Status::OK();
  }
};

class CommandMemory : public CommandDisk {};

class CommandRole : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    *output = srv->GetRoleInfo();
    return Status::OK();
  }
};

class CommandDBSize : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if (args_.size() == 1) {
      KeyNumStats stats;
      srv->GetLatestKeyNumStats(ns, &stats);
      *output = redis::Integer(stats.n_key);
    } else if (args_.size() == 2 && util::EqualICase(args_[1], "scan")) {
      Status s = srv->AsyncScanDBSize(ns);
      if (s.IsOK()) {
        *output = redis::RESP_OK;
      } else {
        return s;
      }
    } else {
      return {Status::RedisExecErr, "DBSIZE subcommand only supports scan"};
    }
    return Status::OK();
  }
};

class CommandPerfLog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "PERFLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    auto perf_log = srv->GetPerfLog();
    if (subcommand_ == "len") {
      *output = redis::Integer(static_cast<int64_t>(perf_log->Size()));
    } else if (subcommand_ == "reset") {
      perf_log->Reset();
      *output = redis::RESP_OK;
    } else if (subcommand_ == "get") {
      *output = perf_log->GetLatestEntries(cnt_);
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandSlowlog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
    }

    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        cnt_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
      }
    }

    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    auto slowlog = srv->GetSlowLog();
    if (subcommand_ == "reset") {
      slowlog->Reset();
      *output = redis::RESP_OK;
      return Status::OK();
    } else if (subcommand_ == "len") {
      *output = redis::Integer(static_cast<int64_t>(slowlog->Size()));
      return Status::OK();
    } else if (subcommand_ == "get") {
      *output = slowlog->GetLatestEntries(cnt_);
      return Status::OK();
    }
    return {Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET"};
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandClient : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    // subcommand: getname id kill list info setname
    if ((subcommand_ == "id" || subcommand_ == "getname" || subcommand_ == "list" || subcommand_ == "info") &&
        args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "setname") && args.size() == 3) {
      // Check if the charset is ok. We need to do this otherwise
      // CLIENT LIST or CLIENT INFO format will break. You should always be able to
      // split by space to get the different fields.
      for (auto ch : args[2]) {
        if (ch < '!' || ch > '~') {
          return {Status::RedisInvalidCmd, "Client names cannot contain spaces, newlines or special characters"};
        }
      }

      conn_name_ = args[2];
      return Status::OK();
    }

    if ((subcommand_ == "kill")) {
      if (args.size() == 2) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      if (args.size() == 3) {
        addr_ = args[2];
        new_format_ = false;
        return Status::OK();
      }

      size_t i = 2;
      new_format_ = true;

      while (i < args.size()) {
        bool more_args = i < args.size();
        if (!strcasecmp(args[i].c_str(), "addr") && more_args) {
          addr_ = args[i + 1];
        } else if (!strcasecmp(args[i].c_str(), "id") && more_args) {
          auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
          if (!parse_result) {
            return {Status::RedisParseErr, errValueNotInteger};
          }

          id_ = *parse_result;
        } else if (!strcasecmp(args[i].c_str(), "skipme") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "yes")) {
            skipme_ = true;
          } else if (!strcasecmp(args[i + 1].c_str(), "no")) {
            skipme_ = false;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else if (!strcasecmp(args[i].c_str(), "type") && more_args) {
          if (!strcasecmp(args[i + 1].c_str(), "normal")) {
            kill_type_ |= kTypeNormal;
          } else if (!strcasecmp(args[i + 1].c_str(), "pubsub")) {
            kill_type_ |= kTypePubsub;
          } else if (!strcasecmp(args[i + 1].c_str(), "master")) {
            kill_type_ |= kTypeMaster;
          } else if (!strcasecmp(args[i + 1].c_str(), "replica") || !strcasecmp(args[i + 1].c_str(), "slave")) {
            kill_type_ |= kTypeSlave;
          } else {
            return {Status::RedisParseErr, errInvalidSyntax};
          }
        } else {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
        i += 2;
      }
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|INFO|KILL ip:port|GETNAME|SETNAME"};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "list") {
      *output = conn->VerbatimString("txt", srv->GetClientsStr());
      return Status::OK();
    } else if (subcommand_ == "info") {
      *output = conn->VerbatimString("txt", conn->ToString());
      return Status::OK();
    } else if (subcommand_ == "setname") {
      conn->SetName(conn_name_);
      *output = redis::RESP_OK;
      return Status::OK();
    } else if (subcommand_ == "getname") {
      std::string name = conn->GetName();
      *output = name == "" ? conn->NilString() : redis::BulkString(name);
      return Status::OK();
    } else if (subcommand_ == "id") {
      *output = redis::Integer(conn->GetID());
      return Status::OK();
    } else if (subcommand_ == "kill") {
      int64_t killed = 0;
      srv->KillClient(&killed, addr_, id_, kill_type_, skipme_, conn);
      if (new_format_) {
        *output = redis::Integer(killed);
      } else {
        if (killed == 0)
          return {Status::RedisExecErr, "No such client"};
        else
          *output = redis::RESP_OK;
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Syntax error, try CLIENT LIST|INFO|KILL ip:port|GETNAME|SETNAME"};
  }

 private:
  std::string addr_;
  std::string conn_name_;
  std::string subcommand_;
  bool skipme_ = false;
  int64_t kill_type_ = 0;
  uint64_t id_ = 0;
  bool new_format_ = true;
};

class CommandMonitor : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    conn->Owner()->BecomeMonitorConn(conn);
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandShutdown : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    if (!srv->IsStopped()) {
      LOG(INFO) << "SHUTDOWN command received, stopping the server";
      srv->Stop();
    }
    return Status::OK();
  }
};

class CommandQuit : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    conn->EnableFlag(redis::Connection::kCloseAfterReply);
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandDebug : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if ((subcommand_ == "sleep") && args.size() == 3) {
      auto second = ParseFloat(args[2]);
      if (!second) {
        return {Status::RedisParseErr, "invalid debug sleep time"};
      }

      microsecond_ = static_cast<uint64_t>(*second * 1000 * 1000);
      return Status::OK();
    } else if (subcommand_ == "protocol" && args.size() == 3) {
      protocol_type_ = util::ToLower(args[2]);
      return Status::OK();
    } else if (subcommand_ == "dbsize-limit" && args.size() == 3) {
      auto val = ParseInt<int32_t>(args[2], {0, 1}, 10);
      if (!val) {
        return {Status::RedisParseErr, "invalid debug dbsize-limit value"};
      }

      dbsize_limit_ = static_cast<bool>(val);
      return Status::OK();
    }
    return {Status::RedisInvalidCmd, "Syntax error, DEBUG SLEEP <seconds>|PROTOCOL <type>|DBSIZE-LIMIT <0|1>"};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "sleep") {
      usleep(microsecond_);
      *output = redis::RESP_OK;
    } else if (subcommand_ == "protocol") {  // protocol type
      if (protocol_type_ == "string") {
        *output = redis::BulkString("Hello World");
      } else if (protocol_type_ == "integer") {
        *output = redis::Integer(12345);
      } else if (protocol_type_ == "double") {
        *output = conn->Double(3.141);
      } else if (protocol_type_ == "array") {
        *output = redis::MultiLen(3);
        for (int i = 0; i < 3; i++) {
          *output += redis::Integer(i);
        }
      } else if (protocol_type_ == "set") {
        *output = conn->HeaderOfSet(3);
        for (int i = 0; i < 3; i++) {
          *output += redis::Integer(i);
        }
      } else if (protocol_type_ == "map") {
        *output = conn->HeaderOfMap(3);
        for (int i = 0; i < 3; i++) {
          *output += redis::Integer(i);
          *output += conn->Bool(i == 1);
        }
      } else if (protocol_type_ == "bignum") {
        *output = conn->BigNumber("1234567999999999999999999999999999999");
      } else if (protocol_type_ == "true") {
        *output = conn->Bool(true);
      } else if (protocol_type_ == "false") {
        *output = conn->Bool(false);
      } else if (protocol_type_ == "null") {
        *output = conn->NilString();
      } else if (protocol_type_ == "attrib") {
        *output = conn->HeaderOfAttribute(1);
        *output += redis::BulkString("key-popularity");
        *output += redis::Array({
            redis::BulkString("key:123"),
            redis::Integer(90),
        });
      } else if (protocol_type_ == "verbatim") {  // verbatim string
        *output = conn->VerbatimString("txt", "verbatim string");
      } else {
        return {Status::RedisErrorNoPrefix,
                "Wrong protocol type name. Please use one of the following: "
                "string|integer|double|array|set|bignum|true|false|null|attrib|verbatim"};
      }
    } else if (subcommand_ == "dbsize-limit") {
      srv->storage->SetDBSizeLimit(dbsize_limit_);
      *output = redis::RESP_OK;
    } else {
      return {Status::RedisInvalidCmd, "Unknown subcommand, should be SLEEP, PROTOCOL or DBSIZE-LIMIT"};
    }
    return Status::OK();
  }

  static uint64_t FlagGen(uint64_t flags, const std::vector<std::string> &args) {
    if (args.size() >= 2 && util::EqualICase(args[1], "protocol")) {
      return flags & ~kCmdExclusive;
    }

    return flags;
  }

 private:
  std::string subcommand_;
  std::string protocol_type_;
  uint64_t microsecond_ = 0;
  bool dbsize_limit_ = false;
};

class CommandCommand : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (args_.size() == 1) {
      CommandTable::GetAllCommandsInfo(output);
    } else {
      std::string sub_command = util::ToLower(args_[1]);
      if ((sub_command == "count" && args_.size() != 2) || (sub_command == "getkeys" && args_.size() < 3) ||
          (sub_command == "info" && args_.size() < 3)) {
        return {Status::RedisExecErr, errWrongNumOfArguments};
      }

      if (sub_command == "count") {
        *output = redis::Integer(CommandTable::Size());
      } else if (sub_command == "info") {
        CommandTable::GetCommandsInfo(output, std::vector<std::string>(args_.begin() + 2, args_.end()));
      } else if (sub_command == "getkeys") {
        auto cmd_iter = CommandTable::GetOriginal()->find(util::ToLower(args_[2]));
        if (cmd_iter == CommandTable::GetOriginal()->end()) {
          return {Status::RedisUnknownCmd, "Invalid command specified"};
        }

        auto key_indexes = GET_OR_RET(CommandTable::GetKeysFromCommand(
            cmd_iter->second, std::vector<std::string>(args_.begin() + 2, args_.end())));

        if (key_indexes.size() == 0) {
          return {Status::RedisExecErr, "Invalid arguments specified for command"};
        }

        std::vector<std::string> keys;
        keys.reserve(key_indexes.size());
        for (const auto &key_index : key_indexes) {
          keys.emplace_back(args_[key_index + 2]);
        }
        *output = conn->MultiBulkString(keys);
      } else {
        return {Status::RedisExecErr, "Command subcommand must be one of COUNT, GETKEYS, INFO"};
      }
    }
    return Status::OK();
  }
};

class CommandEcho : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    *output = redis::BulkString(args_[1]);
    return Status::OK();
  }
};

class CommandTime : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    uint64_t now = util::GetTimeStampUS();
    uint64_t s = now / 1000 / 1000;         // unix time in seconds.
    uint64_t us = now - (s * 1000 * 1000);  // microseconds.

    *output = redis::MultiLen(2);
    *output += redis::BulkString(std::to_string(s));
    *output += redis::BulkString(std::to_string(us));

    return Status::OK();
  }
};

/*
 * HELLO [<protocol-version> [AUTH [<password>|<username> <password>]] [SETNAME <name>] ]
 *   Note that the <username> should always be `default` if provided otherwise AUTH fails.
 *   And it is only meant to be aligning syntax with Redis HELLO.
 */
class CommandHello final : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    size_t next_arg = 1;
    int protocol = 2;  // default protocol version is 2
    if (args_.size() >= 2) {
      auto parse_result = ParseInt<int>(args_[next_arg], 10);
      ++next_arg;
      if (!parse_result) {
        return {Status::NotOK, "Protocol version is not an integer or out of range"};
      }

      protocol = *parse_result;

      // In redis, it will check protocol < 2 or protocol > 3,
      // kvrocks only supports REPL2 by now, but for supporting some
      // `hello 3`, it will not report error when using 3.
      if (protocol < 2 || protocol > 3) {
        return {Status::RedisNoProto, "unsupported protocol version"};
      }
    }

    // Handling AUTH and SETNAME
    for (; next_arg < args_.size(); ++next_arg) {
      size_t more_args = args_.size() - next_arg - 1;
      const std::string &opt = args_[next_arg];
      if (util::ToLower(opt) == "auth" && more_args != 0) {
        if (more_args == 2 || more_args == 4) {
          if (args_[next_arg + 1] != "default") {
            return {Status::NotOK, "Invalid password"};
          }
          next_arg++;
        }
        const auto &user_password = args_[next_arg + 1];
        std::string ns;
        AuthResult auth_result = srv->AuthenticateUser(user_password, &ns);
        switch (auth_result) {
          case AuthResult::NO_REQUIRE_PASS:
            return {Status::NotOK, "Client sent AUTH, but no password is set"};
          case AuthResult::INVALID_PASSWORD:
            return {Status::NotOK, "Invalid password"};
          case AuthResult::IS_USER:
            conn->BecomeUser();
            break;
          case AuthResult::IS_ADMIN:
            conn->BecomeAdmin();
            break;
        }
        conn->SetNamespace(ns);
        next_arg += 1;
      } else if (util::ToLower(opt) == "setname" && more_args != 0) {
        const std::string &name = args_[next_arg + 1];
        conn->SetName(name);
        next_arg += 1;
      } else {
        return {Status::RedisExecErr, "Syntax error in HELLO option " + opt};
      }
    }

    std::vector<std::string> output_list;
    output_list.push_back(redis::BulkString("server"));
    output_list.push_back(redis::BulkString("redis"));
    output_list.push_back(redis::BulkString("version"));
    // What the client want is the Redis compatible version instead of the Kvrocks version.
    output_list.push_back(redis::BulkString(REDIS_VERSION));
    output_list.push_back(redis::BulkString("proto"));
    if (srv->GetConfig()->resp3_enabled) {
      output_list.push_back(redis::Integer(protocol));
      conn->SetProtocolVersion(protocol == 3 ? RESP::v3 : RESP::v2);
    } else {
      output_list.push_back(redis::Integer(2));
    }

    output_list.push_back(redis::BulkString("mode"));
    // Note: sentinel is not supported in kvrocks.
    if (srv->GetConfig()->cluster_enabled) {
      output_list.push_back(redis::BulkString("cluster"));
    } else {
      output_list.push_back(redis::BulkString("standalone"));
    }
    output_list.push_back(redis::BulkString("role"));
    output_list.push_back(redis::BulkString(srv->IsSlave() ? "slave" : "master"));
    // For Kvrocks, the modules is not supported.
    output_list.push_back(redis::BulkString("modules"));
    output_list.push_back(conn->NilArray());
    *output = conn->HeaderOfMap(output_list.size() / 2);
    for (const auto &item : output_list) {
      *output += item;
    }
    return Status::OK();
  }
};

class CommandScan : public CommandScanBase {
 public:
  CommandScan() : CommandScanBase() {}

  static std::string GenerateOutput(Server *srv, [[maybe_unused]] const Connection *conn,
                                    const std::vector<std::string> &keys, const std::string &end_cursor) {
    std::vector<std::string> list;
    if (!end_cursor.empty()) {
      list.emplace_back(
          redis::BulkString(srv->GenerateCursorFromKeyName(end_cursor, CursorType::kTypeBase, kCursorPrefix)));
    } else {
      list.emplace_back(redis::BulkString("0"));
    }

    list.emplace_back(ArrayOfBulkStrings(keys));

    return redis::Array(list);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis_db(srv->storage, conn->GetNamespace());
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeBase);

    std::vector<std::string> keys;
    std::string end_key;

    const auto s = redis_db.Scan(ctx, key_name, limit_, prefix_, suffix_glob_, &keys, &end_key, type_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = GenerateOutput(srv, conn, keys, end_key);
    return Status::OK();
  }
};

class CommandRandomKey : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string key;
    auto cursor = srv->GetLastRandomKeyCursor();
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.RandomKey(ctx, cursor, &key);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    srv->SetLastRandomKeyCursor(key);
    *output = redis::BulkString(key);
    return Status::OK();
  }
};

class CommandCompact : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string begin_key, end_key;
    auto ns = conn->GetNamespace();
    if (ns != kDefaultNamespace) {
      begin_key = ComposeNamespaceKey(ns, "", false);
      end_key = util::StringNext(begin_key);
    }

    Status s = srv->AsyncCompactDB(begin_key, end_key);
    if (!s.IsOK()) return s;

    *output = redis::RESP_OK;
    LOG(INFO) << "Compact was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandBGSave : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    Status s = srv->AsyncBgSaveDB();
    if (!s.IsOK()) return s;

    *output = redis::RESP_OK;
    LOG(INFO) << "BGSave was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandFlushBackup : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    Status s = srv->AsyncPurgeOldBackups(0, 0);
    if (!s.IsOK()) return s;

    *output = redis::RESP_OK;
    LOG(INFO) << "flushbackup was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandSlaveOf : public Commander {
 public:
  static Status IsTryingToReplicateItself(Server *srv, const std::string &host, uint32_t port) {
    auto ip_addresses = util::LookupHostByName(host);
    if (!ip_addresses) {
      return {Status::NotOK, "Can not resolve hostname: " + host};
    }
    for (auto &ip : *ip_addresses) {
      if (util::MatchListeningIP(srv->GetConfig()->binds, ip) && port == srv->GetConfig()->port) {
        return {Status::NotOK, "can't replicate itself"};
      }
      for (std::pair<std::string, uint32_t> &host_port_pair : srv->GetSlaveHostAndPort()) {
        if (host_port_pair.first == ip && host_port_pair.second == port) {
          return {Status::NotOK, "can't replicate your own replicas"};
        }
      }
    }
    return Status::OK();
  }

  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    const auto &port = args[2];
    if (util::ToLower(host_) == "no" && util::ToLower(port) == "one") {
      host_.clear();
      return Status::OK();
    }

    auto parse_result = ParseInt<uint32_t>(port, 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "port should be number"};
    }

    port_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (srv->GetConfig()->cluster_enabled) {
      return {Status::RedisExecErr, "can't change to slave in cluster mode"};
    }

    if (srv->GetConfig()->rocks_db.write_options.disable_wal) {
      return {Status::RedisExecErr, "slaveof doesn't work with disable_wal option"};
    }

    if (host_.empty()) {
      auto s = srv->RemoveMaster();
      if (!s.IsOK()) {
        return s.Prefixed("failed to remove master");
      }

      *output = redis::RESP_OK;
      LOG(WARNING) << "MASTER MODE enabled (user request from '" << conn->GetAddr() << "')";

      return Status::OK();
    }

    auto s = IsTryingToReplicateItself(srv, host_, port_);
    if (!s.IsOK()) return s;
    s = srv->AddMaster(host_, port_, false);
    if (s.IsOK()) {
      *output = redis::RESP_OK;
      LOG(WARNING) << "SLAVE OF " << host_ << ":" << port_ << " enabled (user request from '" << conn->GetAddr()
                   << "')";
    } else {
      LOG(ERROR) << "SLAVE OF " << host_ << ":" << port_ << " (user request from '" << conn->GetAddr()
                 << "') encounter error: " << s.Msg();
    }

    return s;
  }

 private:
  std::string host_;
  uint32_t port_ = 0;
};

class CommandStats : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    std::string stats_json = srv->GetRocksDBStatsJson();
    *output = redis::BulkString(stats_json);
    return Status::OK();
  }
};

static uint64_t GenerateConfigFlag(uint64_t flags, const std::vector<std::string> &args) {
  if (args.size() >= 2 && util::EqualICase(args[1], "set")) {
    return flags | kCmdExclusive;
  }

  return flags;
}

class CommandLastSave : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    int64_t unix_sec = srv->GetLastBgsaveTime();
    *output = redis::Integer(unix_sec);
    return Status::OK();
  }
};

class CommandRestore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    ttl_ms_ = GET_OR_RET(ParseInt<int64_t>(args[2], {0, INT64_MAX}, 10));
    while (parser.Good()) {
      if (parser.EatEqICase("replace")) {
        replace_ = true;
      } else if (parser.EatEqICase("absttl")) {
        absttl_ = true;
      } else if (parser.EatEqICase("idletime")) {
        // idle time is not supported in Kvrocks, so just skip it
        auto idle_time = GET_OR_RET(parser.TakeInt());
        if (idle_time < 0) {
          return {Status::RedisParseErr, "IDLETIME can't be negative"};
        }
      } else if (parser.EatEqICase("freq")) {
        // freq is not supported in Kvrocks, so just skip it
        auto freq = GET_OR_RET(parser.TakeInt());
        if (freq < 0 || freq > 255) {
          return {Status::RedisParseErr, "FREQ must be >= 0 and <= 255"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    rocksdb::Status db_status;
    redis::Database redis(srv->storage, conn->GetNamespace());

    if (!replace_) {
      int count = 0;
      db_status = redis.Exists(ctx, {args_[1]}, &count);
      if (!db_status.ok()) {
        return {Status::RedisExecErr, db_status.ToString()};
      }
      if (count > 0) {
        return {Status::RedisExecErr, "target key name already exists."};
      }
    } else {
      db_status = redis.Del(ctx, args_[1]);
      if (!db_status.ok() && !db_status.IsNotFound()) {
        return {Status::RedisExecErr, db_status.ToString()};
      }
    }
    if (ttl_ms_ && absttl_) {
      auto now = util::GetTimeStampMS();
      if (ttl_ms_ <= now) {
        // return ok if the ttl is already expired
        *output = redis::RESP_OK;
        return Status::OK();
      }
      ttl_ms_ -= now;
    }

    auto stream_ptr = std::make_unique<RdbStringStream>(args_[3]);
    RDB rdb(srv->storage, conn->GetNamespace(), std::move(stream_ptr));
    auto s = rdb.Restore(ctx, args_[1], args_[3], ttl_ms_);
    if (!s.IsOK()) return s;
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  bool replace_ = false;
  bool absttl_ = false;
  uint64_t ttl_ms_ = 0;
};

// command format: rdb load <path> [NX]  [DB index]
class CommandRdb : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    type_ = GET_OR_RET(parser.TakeStr());
    if (!util::EqualICase(type_, "load")) {
      return {Status::RedisParseErr, "unknown subcommand"};
    }

    path_ = GET_OR_RET(parser.TakeStr());
    while (parser.Good()) {
      if (parser.EatEqICase("NX")) {
        overwrite_exist_key_ = false;
      } else if (parser.EatEqICase("DB")) {
        db_index_ = GET_OR_RET(parser.TakeInt<uint32_t>());
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto stream_ptr = std::make_unique<RdbFileStream>(path_);
    GET_OR_RET(stream_ptr->Open());

    RDB rdb(srv->storage, conn->GetNamespace(), std::move(stream_ptr));
    GET_OR_RET(rdb.LoadRdb(ctx, db_index_, overwrite_exist_key_));

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string type_;
  std::string path_;
  bool overwrite_exist_key_ = true;  // default overwrite exist key
  uint32_t db_index_ = 0;
};

class CommandReset : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    // 1. Discards the current MULTI transaction block, if one exists.
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      conn->ResetMultiExec();
    }
    // 2. Unwatches all keys WATCHed by the connection.
    srv->ResetWatchedKeys(conn);
    // 3. Disables CLIENT TRACKING, if in use. (not yet supported)
    // 4. Sets the connection to READWRITE mode.
    // 5. Cancels the connection's ASKING mode, if previously set. (not yet supported)
    // 6. Sets CLIENT REPLY to ON. (not yet supported)
    // 9. Exits MONITOR mode, when applicable.
    if (conn->IsFlagEnabled(Connection::kMonitor)) {
      conn->Owner()->QuitMonitorConn(conn);
    }
    // 10. Aborts Pub/Sub's subscription state (SUBSCRIBE and PSUBSCRIBE), when appropriate.
    if (conn->SubscriptionsCount() != 0) {
      conn->UnsubscribeAll();
    }
    if (conn->PSubscriptionsCount() != 0) {
      conn->PUnsubscribeAll();
    }
    // 11. Deauthenticates the connection, requiring a call AUTH to reauthenticate when authentication is enabled.
    conn->SetNamespace(kDefaultNamespace);
    conn->BecomeAdmin();
    // 12. Turns off NO-EVICT / NO-TOUCH mode. (not yet supported)
    *output = redis::SimpleString("RESET");
    return Status::OK();
  }
};

class CommandApplyBatch : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    raw_batch_ = args[1];
    if (args.size() > 2) {
      if (args.size() > 3) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      if (!util::EqualICase(args[2], "lowpri")) {
        return {Status::RedisParseErr, "only support LOWPRI option"};
      }
      low_pri_ = true;
    }
    return Commander::Parse(args);
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *svr, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    size_t size = raw_batch_.size();
    auto options = svr->storage->DefaultWriteOptions();
    options.low_pri = low_pri_;
    auto s = svr->storage->ApplyWriteBatch(options, std::move(raw_batch_));
    if (!s.IsOK()) return s;

    *output = redis::Integer(size);
    return Status::OK();
  }

 private:
  std::string raw_batch_;
  bool low_pri_ = false;
};

class CommandDump : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() != 2) {
      return {Status::RedisExecErr, errWrongNumOfArguments};
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    rocksdb::Status db_status;
    std::string &key = args_[1];
    redis::Database redis(srv->storage, conn->GetNamespace());
    int count = 0;

    db_status = redis.Exists(ctx, {key}, &count);
    if (!db_status.ok()) {
      if (db_status.IsNotFound()) {
        *output = conn->NilString();
        return Status::OK();
      }
      return {Status::RedisExecErr, db_status.ToString()};
    }
    if (count == 0) {
      *output = conn->NilString();
      return Status::OK();
    }

    RedisType type = kRedisNone;
    db_status = redis.Type(ctx, key, &type);
    if (!db_status.ok()) return {Status::RedisExecErr, db_status.ToString()};

    std::string result;
    auto stream_ptr = std::make_unique<RdbStringStream>(result);
    RDB rdb(srv->storage, conn->GetNamespace(), std::move(stream_ptr));
    auto s = rdb.Dump(key, type);
    if (!s.IsOK()) return s;
    CHECK(dynamic_cast<RdbStringStream *>(rdb.GetStream().get()) != nullptr);
    *output = redis::BulkString(static_cast<RdbStringStream *>(rdb.GetStream().get())->GetInput());
    return Status::OK();
  }
};

class CommandPollUpdates : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    sequence_ = GET_OR_RET(parser.TakeInt<uint64_t>());

    while (parser.Good()) {
      if (parser.EatEqICase("MAX")) {
        max_ = GET_OR_RET(parser.TakeInt<int64_t>(NumericRange<int64_t>{1, 1000}));
      } else if (parser.EatEqICase("STRICT")) {
        is_strict_ = true;
      } else if (parser.EatEqICase("FORMAT")) {
        auto format = GET_OR_RET(parser.TakeStr());
        if (util::EqualICase(format, "RAW")) {
          format_ = Format::Raw;
        } else if (util::EqualICase(format, "RESP")) {
          format_ = Format::RESP;
        } else {
          return {Status::RedisParseErr, "invalid FORMAT option, should be RAW or RESP"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Status::OK();
  }

  static StatusOr<std::string> ToRespFormat(const engine::Storage *storage,
                                            const std::vector<rocksdb::BatchResult> &batches) {
    std::map<std::string, std::vector<std::string>> ns_commands;
    for (const auto &batch : batches) {
      rocksdb::WriteBatch write_batch(batch.writeBatchPtr->Data());
      WriteBatchExtractor extractor(storage->IsSlotIdEncoded(), -1, true);
      auto db_status = write_batch.Iterate(&extractor);
      if (!db_status.ok()) {
        return {Status::RedisExecErr, db_status.ToString()};
      }
      auto parsed_commands = extractor.GetRESPCommands();
      for (const auto &iter : *parsed_commands) {
        // Add the command vector to the namespace
        ns_commands[iter.first].insert(ns_commands[iter.first].end(), iter.second.begin(), iter.second.end());
      }
    }

    std::string updates = MultiLen(ns_commands.size() * 2);
    for (const auto &iter : ns_commands) {
      if (iter.first == kDefaultNamespace) {
        updates += BulkString("default");
      } else {
        updates += BulkString(iter.first);
      }
      updates += Array(iter.second);
    }
    return updates;
  }

  static StatusOr<std::string> ToRawFormat(const std::vector<rocksdb::BatchResult> &batches) {
    std::string updates = redis::MultiLen(batches.size());
    for (const auto &batch : batches) {
      updates += BulkString(util::StringToHex(batch.writeBatchPtr->Data()));
    }
    return updates;
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t next_sequence = sequence_;
    // sequence + 1 is for excluding the current sequence to avoid getting duplicate updates
    auto batches = GET_OR_RET(srv->PollUpdates(sequence_ + 1, max_, is_strict_));
    if (batches.size() > 0) {
      auto &last_batch = batches.back();
      next_sequence = last_batch.sequence + last_batch.writeBatchPtr->Count() - 1;
    }
    std::string updates;
    if (format_ == Format::RESP) {
      updates = GET_OR_RET(ToRespFormat(srv->storage, batches));
    } else {
      updates = GET_OR_RET(ToRawFormat(batches));
    }
    *output = conn->Map({
        {redis::BulkString("latest_sequence"), redis::Integer(srv->storage->LatestSeqNumber())},
        {redis::BulkString("updates"), std::move(updates)},
        {redis::BulkString("next_sequence"), redis::Integer(next_sequence)},
    });
    return Status::OK();
  }

 private:
  enum class Format {
    Raw,
    RESP,
  };

  uint64_t sequence_ = -1;
  bool is_strict_ = false;
  int64_t max_ = 16;
  Format format_ = Format::Raw;
};

REDIS_REGISTER_COMMANDS(Server, MakeCmdAttr<CommandAuth>("auth", 2, "read-only ok-loading auth", NO_KEY),
                        MakeCmdAttr<CommandPing>("ping", -1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandSelect>("select", 2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandInfo>("info", -1, "read-only ok-loading", NO_KEY),
                        MakeCmdAttr<CommandRole>("role", 1, "read-only ok-loading", NO_KEY),
                        MakeCmdAttr<CommandConfig>("config", -2, "read-only admin", NO_KEY, GenerateConfigFlag),
                        MakeCmdAttr<CommandNamespace>("namespace", -3, "read-only admin", NO_KEY),
                        MakeCmdAttr<CommandKeys>("keys", 2, "read-only slow", NO_KEY),
                        MakeCmdAttr<CommandFlushDB>("flushdb", 1, "write no-dbsize-check exclusive", NO_KEY),
                        MakeCmdAttr<CommandFlushAll>("flushall", 1, "write no-dbsize-check exclusive admin", NO_KEY),
                        MakeCmdAttr<CommandDBSize>("dbsize", -1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandSlowlog>("slowlog", -2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandPerfLog>("perflog", -2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandClient>("client", -2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandMonitor>("monitor", 1, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandShutdown>("shutdown", 1, "read-only exclusive no-multi no-script admin",
                                                     NO_KEY),
                        MakeCmdAttr<CommandQuit>("quit", 1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandScan>("scan", -2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandRandomKey>("randomkey", 1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandDebug>("debug", -2, "read-only exclusive", NO_KEY, CommandDebug::FlagGen),
                        MakeCmdAttr<CommandCommand>("command", -1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandEcho>("echo", 2, "read-only", NO_KEY),
                        MakeCmdAttr<CommandTime>("time", 1, "read-only ok-loading", NO_KEY),
                        MakeCmdAttr<CommandDisk>("disk", 3, "read-only", 2, 2, 1),
                        MakeCmdAttr<CommandMemory>("memory", 3, "read-only", 2, 2, 1),
                        MakeCmdAttr<CommandHello>("hello", -1, "read-only ok-loading auth", NO_KEY),
                        MakeCmdAttr<CommandRestore>("restore", -4, "write", 1, 1, 1),

                        MakeCmdAttr<CommandCompact>("compact", 1, "read-only no-script", NO_KEY),
                        MakeCmdAttr<CommandBGSave>("bgsave", 1, "read-only no-script admin", NO_KEY),
                        MakeCmdAttr<CommandLastSave>("lastsave", 1, "read-only admin", NO_KEY),
                        MakeCmdAttr<CommandFlushBackup>("flushbackup", 1, "read-only no-script admin", NO_KEY),
                        MakeCmdAttr<CommandSlaveOf>("slaveof", 3, "read-only exclusive no-script admin", NO_KEY),
                        MakeCmdAttr<CommandSlaveOf>("replicaof", 3, "read-only exclusive no-script admin", NO_KEY),
                        MakeCmdAttr<CommandStats>("stats", 1, "read-only", NO_KEY),
                        MakeCmdAttr<CommandRdb>("rdb", -3, "write exclusive admin", NO_KEY),
                        MakeCmdAttr<CommandReset>("reset", 1, "ok-loading bypass-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandApplyBatch>("applybatch", -2, "write no-multi", NO_KEY),
                        MakeCmdAttr<CommandDump>("dump", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandPollUpdates>("pollupdates", -2, "read-only admin", NO_KEY), )
}  // namespace redis
