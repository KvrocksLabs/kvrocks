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
#include "string_util.h"
#include "types/redis_tdigest.h"

namespace redis {
namespace {
constexpr auto kCompressionArg = "compression";
constexpr auto kErrWrongKeyword = "T-Digest: wrong keyword";
constexpr auto kErrWrongNumOfCreateArguments = "wrong number of arguments for 'tdigest.create' command";
constexpr auto kErrWrongNumOfInfoArguments = "wrong number of arguments for 'tdigest.info' command";
constexpr auto kErrParseCompression = "T-Digest: error parsing compression parameter";
constexpr auto kErrCompressionMustBePositive = "T-Digest: compression parameter needs to be a positive integer";
const static auto kErrCompressionOutOfRange =
    "T-Digest: compression must be between 1 and " + std::to_string(kTDigestMaxCompression);
constexpr auto kErrKeyNotFound = "T-Digest: key does not exist";
constexpr auto kErrKeyAlreadyExists = "T-Digest: key already exists";

constexpr auto kInfoCompression = "Compression";
constexpr auto kInfoCapacity = "Capacity";
constexpr auto kInfoMergedNodes = "Merged nodes";
constexpr auto kInfoUnmergedNodes = "Unmerged nodes";
constexpr auto kInfoMergedWeight = "Merged weight";
constexpr auto kInfoUnmergedWeight = "Unmerged weight";
constexpr auto kInfoObservations = "Observations";
constexpr auto kInfoTotalCompressions = "Total compressions";
constexpr auto kInfoMemoryUsage = "Memory usage";
}  // namespace

class CommandTDigestCreate : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_name_ = GET_OR_RET(parser.TakeStr());
    options_.compression = 100;
    bool invalid_keyword = false;
    if (args.size() != 2 && (args.size() != 4 || (invalid_keyword = !util::EqualICase(kCompressionArg, args[2])))) {
      return {Status::RedisParseErr, invalid_keyword ? kErrWrongKeyword : kErrWrongNumOfCreateArguments};
    }
    if (parser.EatEqICase(kCompressionArg)) {
      auto status = parser.TakeInt<int32_t>();
      if (!status) {
        return {Status::RedisParseErr, kErrParseCompression};
      }
      auto compression = *status;
      if (compression <= 0) {
        return {Status::RedisParseErr, kErrCompressionMustBePositive};
      }
      if (compression < 1 || compression > static_cast<int32_t>(kTDigestMaxCompression)) {
        return {Status::RedisParseErr, kErrCompressionOutOfRange};
      }
      options_.compression = static_cast<uint32_t>(compression);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    bool exists = false;
    auto s = tdigest.Create(ctx, key_name_, options_, &exists);
    if (!s.ok()) {
      if (exists) {
        return {Status::RedisExecErr, kErrKeyAlreadyExists};
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
    if (args.size() != 2) {
      return {Status::RedisParseErr, kErrWrongNumOfInfoArguments};
    }
    key_name_ = args[1];
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    TDigestMetadata metadata;
    auto s = tdigest.GetMetaData(ctx, key_name_, &metadata);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisExecErr, kErrKeyNotFound};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(conn->HeaderOfMap(9));
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
    output->append(redis::BulkString(kInfoMemoryUsage));
    output->append(redis::Integer(sizeof(TDigest)));
    return Status::OK();
  }

 private:
  std::string key_name_;
};

REDIS_REGISTER_COMMANDS(TDigest, MakeCmdAttr<CommandTDigestCreate>("tdigest.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestInfo>("tdigest.info", 2, "read-only", 1, 1, 1));
}  // namespace redis