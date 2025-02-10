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

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "scan_base.h"
#include "server/server.h"
#include "time_util.h"
#include "types/redis_hash.h"

namespace redis {

class CommandHGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, true, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Delete(ctx, args_[1], fields, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t count = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Size(ctx, args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[3], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrBy(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandHIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[3]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrByFloat(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MGet(ctx, args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
      *output = conn->MultiBulkString(values);
    } else {
      *output = conn->MultiBulkString(values, statuses);
    }
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, false, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (GetAttributes()->name == "hset") {
      *output = redis::Integer(ret);
    } else {
      *output = redis::RESP_OK;
    }
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHSetExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    ttl_ = GET_OR_RET(ParseInt<uint64_t>(args[2], 10));
    if ((args.size() - 3) % 2 != 0) {
      return {Status::RedisParseErr, "Invalid number of arguments: field-value pairs must be complete"};
    }
    for (size_t i = 3; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, false, &ret, ttl_ * 1000 + util::GetTimeStampMS());
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
  uint64_t ttl_ = 0;
};

class CommandHKeys : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyKey);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> keys;
    keys.reserve(field_values.size());
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = conn->MultiBulkString(keys);

    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyValue);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    values.reserve(field_values.size());
    for (const auto &p : field_values) {
      values.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(values);

    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> kv_pairs;
    kv_pairs.reserve(field_values.size());
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = conn->MapOfBulkStrings(kv_pairs);

    return Status::OK();
  }
};

class CommandHRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("REV")) {
        spec_.reversed = true;
      } else if (parser.EatEqICase("LIMIT")) {
        spec_.offset = GET_OR_RET(parser.TakeInt());
        spec_.count = GET_OR_RET(parser.TakeInt());
      } else {
        return parser.InvalidSyntax();
      }
    }
    if (spec_.reversed) {
      return ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      return ParseRangeLexSpec(args[2], args[3], &spec_);
    }
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    rocksdb::Status s = hash_db.RangeByLex(ctx, args_[1], spec_, &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> kv_pairs;
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(kv_pairs);

    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  CommandHScan() = default;
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeHash);

    auto s = hash_db.Scan(ctx, key_, key_name, limit_, prefix_, &fields, &values);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto cursor = GetNextCursor(srv, fields, CursorType::kTypeHash);
    std::vector<std::string> entries;
    entries.reserve(2 * fields.size());
    for (size_t i = 0; i < fields.size(); i++) {
      entries.emplace_back(redis::BulkString(fields[i]));
      entries.emplace_back(redis::BulkString(values[i]));
    }
    *output = redis::Array({redis::BulkString(cursor), redis::Array(entries)});
    return Status::OK();
  }
};

class CommandHRandField : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 3) {
      no_parameters_ = false;
      auto parse_result = ParseInt<int64_t>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      command_count_ = *parse_result;

      if (args.size() > 4 || (args.size() == 4 && !util::EqualICase(args[3], "withvalues"))) {
        return {Status::RedisParseErr, errInvalidSyntax};
      } else if (args.size() == 4) {
        withvalues_ = true;
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.RandField(ctx, args_[1], command_count_, &field_values,
                               withvalues_ ? HashFetchType::kAll : HashFetchType::kOnlyKey);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_entries;
    result_entries.reserve(field_values.size());
    for (const auto &p : field_values) {
      result_entries.emplace_back(p.field);
      if (withvalues_) result_entries.emplace_back(p.value);
    }

    if (no_parameters_)
      *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(result_entries[0]);
    else
      *output = ArrayOfBulkStrings(result_entries);
    return Status::OK();
  }

 private:
  bool withvalues_ = false;
  int64_t command_count_ = 1;
  bool no_parameters_ = true;
};

class CommandFieldExpireBase : public Commander {
 protected:
  Status commonParse(const std::vector<std::string> &args, int start_idx) {
    CommandParser parser(args, start_idx);
    std::string_view expire_flag, num_flag;
    uint64_t fields_num = 0;
    while (parser.Good()) {
      if (parser.EatEqICaseFlag("FIELDS", num_flag)) {
        fields_num = GET_OR_RET(parser.template TakeInt<uint64_t>());
        break;
      } else if (parser.EatEqICaseFlag("NX", expire_flag)) {
        field_expire_type_ = HashFieldExpireType::NX;
      } else if (parser.EatEqICaseFlag("XX", expire_flag)) {
        field_expire_type_ = HashFieldExpireType::XX;
      } else if (parser.EatEqICaseFlag("GT", expire_flag)) {
        field_expire_type_ = HashFieldExpireType::GT;
      } else if (parser.EatEqICaseFlag("LT", expire_flag)) {
        field_expire_type_ = HashFieldExpireType::LT;
      } else {
        return parser.InvalidSyntax();
      }
    }

    auto remains = parser.Remains();
    auto size = args.size();
    if (remains != fields_num) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = size - remains; i < size; i++) {
      fields_.emplace_back(args_[i]);
    }

    return Status::OK();
  }

  Status expireFieldExecute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) {
    if (!srv->storage->GetConfig()->hash_field_expiration) {
      return {Status::RedisExecErr, "field expiration feature is disabled"};
    }

    std::vector<int8_t> ret;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.ExpireFields(ctx, args_[1], expire_, fields_, field_expire_type_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::MultiLen(ret.size());
    for (const auto &i : ret) {
      output->append(redis::Integer(i));
    }

    return Status::OK();
  }

  Status ttlExpireExecute(engine::Context &ctx, Server *srv, Connection *conn, std::vector<int64_t> &ret) {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.TTLFields(ctx, args_[1], fields_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    return Status::OK();
  }

  uint64_t expire_ = 0;
  HashFieldExpireType field_expire_type_ = HashFieldExpireType::None;
  std::vector<Slice> fields_;
};

class CommandHExpire : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<uint64_t>(args[2], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    expire_ = *parse_result * 1000 + util::GetTimeStampMS();
    return CommandFieldExpireBase::commonParse(args, 3);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    return expireFieldExecute(ctx, srv, conn, output);
  }
};

class CommandHExpireAt : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<uint64_t>(args[2], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    expire_ = *parse_result * 1000;
    return CommandFieldExpireBase::commonParse(args, 3);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    return expireFieldExecute(ctx, srv, conn, output);
  }
};

class CommandHPExpire : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<uint64_t>(args[2], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    expire_ = *parse_result + util::GetTimeStampMS();
    return CommandFieldExpireBase::commonParse(args, 3);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    return expireFieldExecute(ctx, srv, conn, output);
  }
};

class CommandHPExpireAt : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<uint64_t>(args[2], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    expire_ = *parse_result;
    return CommandFieldExpireBase::commonParse(args, 3);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    return expireFieldExecute(ctx, srv, conn, output);
  }
};

class CommandHExpireTime : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override { return CommandFieldExpireBase::commonParse(args, 2); }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int64_t> ret;
    auto s = ttlExpireExecute(ctx, srv, conn, ret);
    if (!s.IsOK()) {
      return {Status::RedisExecErr, s.Msg()};
    }
    auto now = util::GetTimeStampMS();
    *output = redis::MultiLen(ret.size());
    for (const auto &ttl : ret) {
      if (ttl > 0) {
        output->append(redis::Integer((now + ttl) / 1000));
      } else {
        output->append(redis::Integer(ttl));
      }
    }
    return Status::OK();
  }
};

class CommandHPExpireTime : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override { return CommandFieldExpireBase::commonParse(args, 2); }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int64_t> ret;
    auto s = ttlExpireExecute(ctx, srv, conn, ret);
    if (!s.IsOK()) {
      return {Status::RedisExecErr, s.Msg()};
    }
    auto now = util::GetTimeStampMS();
    *output = redis::MultiLen(ret.size());
    for (const auto &ttl : ret) {
      if (ttl > 0) {
        output->append(redis::Integer(now + ttl));
      } else {
        output->append(redis::Integer(ttl));
      }
    }
    return Status::OK();
  }
};

class CommandHTTL : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override { return CommandFieldExpireBase::commonParse(args, 2); }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int64_t> ret;
    auto s = ttlExpireExecute(ctx, srv, conn, ret);
    if (!s.IsOK()) {
      return {Status::RedisExecErr, s.Msg()};
    }
    *output = redis::MultiLen(ret.size());
    for (const auto &ttl : ret) {
      output->append(redis::Integer(ttl > 0 ? ttl / 1000 : ttl));
    }
    return Status::OK();
  }
};

class CommandHPTTL : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override { return CommandFieldExpireBase::commonParse(args, 2); }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int64_t> ret;
    auto s = ttlExpireExecute(ctx, srv, conn, ret);
    if (!s.IsOK()) {
      return {Status::RedisExecErr, s.Msg()};
    }
    *output = redis::MultiLen(ret.size());
    for (const auto &ttl : ret) {
      output->append(redis::Integer(ttl));
    }
    return Status::OK();
  }
};

class CommandHPersist : public CommandFieldExpireBase {
 public:
  Status Parse(const std::vector<std::string> &args) override { return CommandFieldExpireBase::commonParse(args, 2); }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<int8_t> ret;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    auto s = hash_db.PersistFields(ctx, args_[1], fields_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::MultiLen(ret.size());
    for (const auto &i : ret) {
      output->append(redis::Integer(i));
    }
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Hash, MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetExpire>("hsetexpire", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetNX>("hsetnx", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHDel>("hdel", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHLen>("hlen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHVals>("hvals", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRandField>("hrandfield", -2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHExpire>("hexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHExpireAt>("hexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHExpireTime>("hexpiretime", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpire>("hpexpire", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireAt>("hpexpireat", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHPExpireTime>("hpexpiretime", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPersist>("hpersist", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHTTL>("httl", -5, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHPTTL>("hpttl", -5, "read-only", 1, 1, 1), )

}  // namespace redis
