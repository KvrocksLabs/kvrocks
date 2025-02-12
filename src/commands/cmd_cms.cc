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

#include <types/cms.h>
#include <types/redis_cms.h>

#include "commander.h"
#include "commands/command_parser.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

/// CMS.INCRBY key item increment [item increment ...]
///
/// The `key` should be an existing Count-Min Sketch key,
/// otherwise, the command will return an error.
///
/// The output should be an array of integers, each integer
/// means the counter value after the increment. If the increment
/// overflows, the return value will be `"CMS: INCRBY overflow"`.
class CommandCMSIncrBy final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if ((args_.size() - 2) % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    // <item, increment> pairs
    std::vector<redis::CMS::IncrByPair> elements;
    elements.reserve((args_.size() - 2) / 2);
    for (size_t i = 2; i < args_.size(); i += 2) {
      std::string_view key = args_[i];
      auto parse_result = ParseInt<int64_t>(args_[i + 1]);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      int64_t value = *parse_result;
      elements.emplace_back(redis::CMS::IncrByPair{key, value});
    }
    std::vector<uint32_t> counters;
    s = cms.IncrBy(ctx, args_[1], elements, &counters);
    if (s.IsNotFound()) {
      return {Status::RedisExecErr, "Key not found"};
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> strs;
    for (uint32_t counter : counters) {
      if (counter == std::numeric_limits<uint32_t>::max()) {
        strs.push_back(redis::Error({Status::RedisExecErr, "CMS: INCRBY overflow"}));
      } else {
        strs.push_back(redis::Integer(counter));
      }
    }
    *output = redis::Array(strs);
    return Status::OK();
  }
};

/// CMS.INFO key
class CommandCMSInfo final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    CMSketch::CMSInfo ret{};

    s = cms.Info(ctx, args_[1], &ret);

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, "Key not found"};
    }

    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Array({redis::BulkString("width"), redis::Integer(ret.width), redis::BulkString("depth"),
                            redis::Integer(ret.depth), redis::BulkString("count"), redis::Integer(ret.count)});

    return Status::OK();
  }
};

/// CMS.INITBYDIM key width depth
///
/// If the key already exists, the command will return an error.
class CommandCMSInitByDim final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;
    auto width_result = ParseInt<uint32_t>(this->args_[2]);
    if (!width_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    uint32_t width = *width_result;

    auto depth_result = ParseInt<uint32_t>(this->args_[3]);
    if (!depth_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    uint32_t depth = *depth_result;

    s = cms.InitByDim(ctx, args_[1], width, depth);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

/// CMS.INITBYPROB key error probability
///
/// If the key already exists, the command will return an error.
class CommandCMSInitByProb final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);
    rocksdb::Status s;

    auto error_result = ParseFloat<double>(args_[2]);
    if (!error_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    double error = *error_result;

    auto delta_result = ParseFloat<double>(args_[3]);
    if (!delta_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    double delta = *delta_result;

    s = cms.InitByProb(ctx, args_[1], error, delta);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

/// CMS.MERGE destination numKeys source [source ...] [WEIGHTS weight [weight ...]]
class CommandCMSMerge final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    destination_ = args[1];

    StatusOr<int> num_key_result = parser.TakeInt();
    if (!num_key_result || *num_key_result <= 0) {
      return {Status::RedisParseErr, "invalid number of source keys"};
    }
    num_keys_ = *num_key_result;

    src_keys_.reserve(num_keys_);
    for (int i = 0; i < num_keys_; i++) {
      auto result = parser.TakeStr();
      if (!result) {
        return {Status::RedisParseErr, "Error parsing source key"};
      }
      src_keys_.emplace_back(std::move(*result));
    }

    bool weights_found = false;
    while (parser.Good()) {
      // Parse "WEIGHTS" if exists.
      if (parser.EatEqICase("WEIGHTS")) {
        if (weights_found) {
          return {Status::RedisParseErr, "WEIGHTS option cannot be specified multiple times"};
        }
        src_weights_.reserve(num_keys_);
        for (int i = 0; i < num_keys_; i++) {
          StatusOr<uint32_t> weight_result = parser.TakeInt<uint32_t>();
          if (!weight_result || *weight_result == 0) {
            return {Status::RedisParseErr, "invalid weight value"};
          }
          src_weights_.emplace_back(*weight_result);
        }
        weights_found = true;
      } else {
        return {Status::RedisParseErr, "Syntax error: unexpected token"};
      }
    }

    if (!weights_found) {
      src_weights_.resize(num_keys_, 1);
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    rocksdb::Status s = cms.MergeUserKeys(ctx, destination_, src_keys_, src_weights_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  Slice destination_;
  int num_keys_{0};
  std::vector<Slice> src_keys_;
  std::vector<uint32_t> src_weights_;
};

/// CMS.QUERY key item [item ...]
class CommandCMSQuery final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CMS cms(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    std::vector<uint32_t> counters{};
    std::vector<std::string> elements;

    for (size_t i = 2; i < args_.size(); ++i) {
      elements.emplace_back(args_[i]);
    }

    rocksdb::Status s = cms.Query(ctx, args_[1], elements, counters);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> output_values;
    output_values.reserve(counters.size());
    for (const auto &counter : counters) {
      output_values.emplace_back(Integer(counter));
    }
    *output = redis::Array(output_values);

    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(CMS, MakeCmdAttr<CommandCMSIncrBy>("cms.incrby", -4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInfo>("cms.info", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInitByDim>("cms.initbydim", 4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSInitByProb>("cms.initbyprob", 4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSMerge>("cms.merge", -4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCMSQuery>("cms.query", -3, "read-only", 0, 0, 0), );
}  // namespace redis