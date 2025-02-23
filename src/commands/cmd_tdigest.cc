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

#include "command_parser.h"
#include "commander.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "status.h"
#include "types/redis_tdigest.h"

namespace redis {
namespace {
constexpr auto kCompressionArg = "compression";

constexpr auto kInfoCompression = "Compression";
constexpr auto kInfoCapacity = "Capacity";
constexpr auto kInfoMergedNodes = "Merged nodes";
constexpr auto kInfoUnmergedNodes = "Unmerged nodes";
constexpr auto kInfoMergedWeight = "Merged weight";
constexpr auto kInfoUnmergedWeight = "Unmerged weight";
constexpr auto kInfoObservations = "Observations";
constexpr auto kInfoTotalCompressions = "Total compressions";
}  // namespace

class CommandTDigestCreate : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_name_ = GET_OR_RET(parser.TakeStr());
    options_.compression = 100;
    if (parser.EatEqICase(kCompressionArg)) {
      if (!parser.Good()) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      auto status = parser.TakeInt<int32_t>();
      if (!status) {
        return {Status::RedisParseErr, errParseCompression};
      }
      auto compression = *status;
      if (compression <= 0) {
        return {Status::RedisParseErr, errCompressionMustBePositive};
      }
      if (compression < 1 || compression > static_cast<int32_t>(kTDigestMaxCompression)) {
        return {Status::RedisParseErr, errCompressionOutOfRange};
      }
      options_.compression = static_cast<uint32_t>(compression);
    }
    if (parser.Good()) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    bool exists = false;
    auto s = tdigest.Create(ctx, key_name_, options_, &exists);
    if (!s.ok()) {
      if (exists) {
        return {Status::RedisExecErr, errKeyAlreadyExists};
      }
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string key_name_;
  TDigestCreateOptions options_;
};

class CommandTDigestInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    key_name_ = args[1];
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestMetadata metadata;
    auto s = tdigest.GetMetaData(ctx, key_name_, &metadata);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, errKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(conn->HeaderOfMap(8));
    output->append(redis::BulkString(kInfoCompression));
    output->append(redis::Integer(metadata.compression));
    output->append(redis::BulkString(kInfoCapacity));
    output->append(redis::Integer(metadata.capacity));
    output->append(redis::BulkString(kInfoMergedNodes));
    output->append(redis::Integer(metadata.merged_nodes));
    output->append(redis::BulkString(kInfoUnmergedNodes));
    output->append(redis::Integer(metadata.unmerged_nodes));
    output->append(redis::BulkString(kInfoMergedWeight));
    output->append(redis::Integer(metadata.merged_weight));
    output->append(redis::BulkString(kInfoUnmergedWeight));
    output->append(redis::Integer(metadata.total_weight - metadata.merged_weight));
    output->append(redis::BulkString(kInfoObservations));
    output->append(redis::Integer(metadata.total_observations));
    output->append(redis::BulkString(kInfoTotalCompressions));
    output->append(redis::Integer(metadata.merge_times));
    // "Memory usage" is not meaningful for kvrocks, so we don't provide it here.
    return Status::OK();
  }

 private:
  std::string key_name_;
};

REDIS_REGISTER_COMMANDS(TDigest, MakeCmdAttr<CommandTDigestCreate>("tdigest.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestInfo>("tdigest.info", 2, "read-only", 1, 1, 1));
}  // namespace redis
