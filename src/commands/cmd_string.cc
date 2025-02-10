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

#include <cstdint>
#include <optional>
#include <string>

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "server/redis_reply.h"
#include "server/redis_request.h"
#include "server/server.h"
#include "storage/redis_db.h"
#include "time_util.h"
#include "ttl_util.h"
#include "types/redis_bitmap.h"
#include "types/redis_string.h"

namespace redis {

class CommandGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.Get(ctx, args_[1], &value);
    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = srv->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());
      s = bitmap_db.GetString(ctx, args_[1], max_btos_size, &value);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandGetEx : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    std::string_view ttl_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseExpireFlags(parser, ttl_flag))) {
        expire_ = *v;
      } else if (parser.EatEqICaseFlag("PERSIST", ttl_flag)) {
        expire_ = 0;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string value;

    redis::String string_db(srv->storage, conn->GetNamespace());
    auto s = string_db.GetEx(ctx, args_[1], &value, expire_);

    // The IsInvalidArgument error means the key type maybe a bitmap
    // which we need to fall back to the bitmap's GetString according
    // to the `max-bitmap-to-string-mb` configuration.
    if (s.IsInvalidArgument()) {
      Config *config = srv->GetConfig();
      uint32_t max_btos_size = static_cast<uint32_t>(config->max_bitmap_to_string_mb) * MiB;
      redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());
      s = bitmap_db.GetString(ctx, args_[1], max_btos_size, &value);
      if (s.ok() && expire_) {
        s = bitmap_db.Expire(ctx, args_[1], expire_.value());
      }
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }

 private:
  std::optional<uint64_t> expire_;
};

class CommandStrlen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.Get(ctx, args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = redis::Integer(0);
    } else {
      *output = redis::Integer(value.size());
    }
    return Status::OK();
  }
};

class CommandGetSet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::optional<std::string> old_value;

    auto s = string_db.GetSet(ctx, args_[1], args_[2], old_value);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (old_value.has_value()) {
      *output = redis::BulkString(old_value.value());
    } else {
      *output = conn->NilString();
    }
    return Status::OK();
  }
};

class CommandGetDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = string_db.GetDel(ctx, args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
    } else {
      *output = redis::BulkString(value);
    }
    return Status::OK();
  }
};

class CommandGetRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string value;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.Get(ctx, args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
      return Status::OK();
    }

    if (start_ < 0) start_ = static_cast<int>(value.size()) + start_;
    if (stop_ < 0) stop_ = static_cast<int>(value.size()) + stop_;
    if (start_ < 0) start_ = 0;
    if (stop_ > static_cast<int>(value.size())) stop_ = static_cast<int>(value.size());
    if (start_ > stop_) {
      *output = conn->NilString();
    } else {
      *output = redis::BulkString(value.substr(start_, stop_ - start_ + 1));
    }
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandSubStr : public CommandGetRange {
 public:
  CommandSubStr() : CommandGetRange() {}
};

class CommandSetRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], {0, INT32_MAX}, 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    offset_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto total = offset_ + args_[3].size();
    if (total > srv->GetConfig()->proto_max_bulk_len) {
      return {Status::RedisExecErr, "string exceeds maximum allowed size"};
    }

    auto s = string_db.SetRange(ctx, args_[1], offset_, args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int offset_ = 0;
};

class CommandMGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> values;
    // always return OK

    auto statuses = string_db.MGet(ctx, keys, &values);
    *output = conn->MultiBulkString(values, statuses);
    return Status::OK();
  }
};

class CommandAppend : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.Append(ctx, args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    std::string_view ttl_flag, set_flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseExpireFlags(parser, ttl_flag))) {
        expire_ = *v;
      } else if (parser.EatEqICaseFlag("KEEPTTL", ttl_flag)) {
        keep_ttl_ = true;
      } else if (parser.EatEqICaseFlag("NX", set_flag)) {
        set_flag_ = StringSetType::NX;
      } else if (parser.EatEqICaseFlag("XX", set_flag)) {
        set_flag_ = StringSetType::XX;
      } else if (parser.EatEqICase("GET")) {
        get_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::optional<std::string> ret;
    redis::String string_db(srv->storage, conn->GetNamespace());

    rocksdb::Status s = string_db.Set(ctx, args_[1], args_[2], {expire_, set_flag_, get_, keep_ttl_}, ret);

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (get_) {
      if (ret.has_value()) {
        *output = redis::BulkString(ret.value());
      } else {
        *output = conn->NilString();
      }
    } else {
      if (ret.has_value()) {
        *output = redis::RESP_OK;
      } else {
        *output = conn->NilString();
      }
    }
    return Status::OK();
  }

 private:
  uint64_t expire_ = 0;
  bool get_ = false;
  bool keep_ttl_ = false;
  StringSetType set_flag_ = StringSetType::NONE;
};

class CommandSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_result <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    expire_ = *parse_result * 1000 + util::GetTimeStampMS();

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.SetEX(ctx, args_[1], args_[3], expire_);
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  uint64_t expire_ = 0;
};

class CommandPSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto ttl_ms = ParseInt<int64_t>(args[2], 10);
    if (!ttl_ms) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*ttl_ms <= 0) return {Status::RedisParseErr, errInvalidExpireTime};

    expire_ = *ttl_ms + util::GetTimeStampMS();

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.SetEX(ctx, args_[1], args_[3], expire_);
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  uint64_t expire_ = 0;
};

class CommandMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    std::vector<StringPair> kvs;
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSet(ctx, kvs, 0);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandSetNX : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    bool ret = false;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.SetNX(ctx, args_[1], args_[2], 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandMSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    bool ret = false;
    std::vector<StringPair> kvs;
    redis::String string_db(srv->storage, conn->GetNamespace());
    for (size_t i = 1; i < args_.size(); i += 2) {
      kvs.emplace_back(StringPair{args_[i], args_[i + 1]});
    }

    auto s = string_db.MSetNX(ctx, kvs, 0, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandIncr : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.IncrBy(ctx, args_[1], 1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.IncrBy(ctx, args_[1], -1, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.IncrBy(ctx, args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.IncrByFloat(ctx, args_[1], increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandDecrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;

    // Negating LLONG_MIN will cause an overflow.
    if (increment_ == LLONG_MIN) {
      return {Status::RedisParseErr, "decrement would overflow"};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::String string_db(srv->storage, conn->GetNamespace());

    auto s = string_db.IncrBy(ctx, args_[1], -1 * increment_, &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandCAS : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    std::string_view flag;
    while (parser.Good()) {
      if (auto v = GET_OR_RET(ParseExpireFlags(parser, flag))) {
        expire_ = *v;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    int ret = 0;

    auto s = string_db.CAS(ctx, args_[1], args_[2], args_[3], expire_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  uint64_t expire_ = 0;
};

class CommandCAD : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());
    int ret = 0;

    auto s = string_db.CAD(ctx, args_[1], args_[2], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandLCS : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    bool get_idx = false;
    bool get_len = false;
    while (parser.Good()) {
      if (parser.EatEqICase("IDX")) {
        get_idx = true;
      } else if (parser.EatEqICase("LEN")) {
        get_len = true;
      } else if (parser.EatEqICase("WITHMATCHLEN")) {
        with_match_len_ = true;
      } else if (parser.EatEqICase("MINMATCHLEN")) {
        min_match_len_ = GET_OR_RET(parser.TakeInt<int64_t>());
        if (min_match_len_ < 0) {
          min_match_len_ = 0;
        }
      } else {
        return parser.InvalidSyntax();
      }
    }

    // Complain if the user passed ambiguous parameters.
    if (get_idx && get_len) {
      return {Status::RedisParseErr,
              "If you want both the length and indexes, "
              "please just use IDX."};
    }

    if (get_len) {
      type_ = StringLCSType::LEN;
    } else if (get_idx) {
      type_ = StringLCSType::IDX;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::String string_db(srv->storage, conn->GetNamespace());

    StringLCSResult rst;

    auto s = string_db.LCS(ctx, args_[1], args_[2], {type_, min_match_len_}, &rst);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // Build output by the rst type.
    if (auto lcs = std::get_if<std::string>(&rst)) {
      *output = redis::BulkString(*lcs);
    } else if (auto len = std::get_if<uint32_t>(&rst)) {
      *output = redis::Integer(*len);
    } else if (auto result = std::get_if<StringLCSIdxResult>(&rst)) {
      *output = conn->HeaderOfMap(2);
      *output += redis::BulkString("matches");
      *output += redis::MultiLen(result->matches.size());
      for (const auto &match : result->matches) {
        *output += redis::MultiLen(with_match_len_ ? 3 : 2);
        *output += redis::MultiLen(2);
        *output += redis::Integer(match.a.start);
        *output += redis::Integer(match.a.end);
        *output += redis::MultiLen(2);
        *output += redis::Integer(match.b.start);
        *output += redis::Integer(match.b.end);
        if (with_match_len_) {
          *output += redis::Integer(match.match_len);
        }
      }
      *output += redis::BulkString("len");
      *output += redis::Integer(result->len);
    }

    return Status::OK();
  }

 private:
  StringLCSType type_ = StringLCSType::NONE;
  bool with_match_len_ = false;
  int64_t min_match_len_ = 0;
};

REDIS_REGISTER_COMMANDS(
    String, MakeCmdAttr<CommandGet>("get", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetEx>("getex", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandStrlen>("strlen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetSet>("getset", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandGetRange>("getrange", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandSubStr>("substr", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandGetDel>("getdel", 2, "write no-dbsize-check", 1, 1, 1),
    MakeCmdAttr<CommandSetRange>("setrange", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandMGet>("mget", -2, "read-only", 1, -1, 1),
    MakeCmdAttr<CommandAppend>("append", 3, "write", 1, 1, 1), MakeCmdAttr<CommandSet>("set", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetEX>("setex", 4, "write", 1, 1, 1), MakeCmdAttr<CommandPSetEX>("psetex", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandSetNX>("setnx", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandMSetNX>("msetnx", -3, "write", 1, -1, 2),
    MakeCmdAttr<CommandMSet>("mset", -3, "write", 1, -1, 2), MakeCmdAttr<CommandIncrBy>("incrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncrByFloat>("incrbyfloat", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandIncr>("incr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandDecrBy>("decrby", 3, "write", 1, 1, 1),
    MakeCmdAttr<CommandDecr>("decr", 2, "write", 1, 1, 1), MakeCmdAttr<CommandCAS>("cas", -4, "write", 1, 1, 1),
    MakeCmdAttr<CommandCAD>("cad", 3, "write", 1, 1, 1), MakeCmdAttr<CommandLCS>("lcs", -3, "read-only", 1, 2, 1), )
}  // namespace redis
