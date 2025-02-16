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

class CommandTDigestCreate : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_name_ = GET_OR_RET(parser.TakeStr());
    options_.compression = 100;
    if (parser.EatEqICase("compression")) {
      auto compression = GET_OR_RET(parser.TakeInt<int32_t>());
      if (compression <= 0) {
        return {Status::RedisParseErr, "TDIGEST: compression parameter needs to be a positive integer"};
      }
      if (compression < 1 || compression > static_cast<int32_t>(kTDigestMaxCompression)) {
        return {Status::RedisParseErr,
                "TDIGEST: compression must be between 1 and " + std::to_string(kTDigestMaxCompression)};
      }
      options_.compression = static_cast<uint32_t>(compression);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    TDigest tdigest(srv->storage, conn->GetNamespace());
    bool exists = false;
    LOG(INFO) << "TDIGEST: Create " << key_name_;
    auto s = tdigest.Create(ctx, key_name_, options_, &exists);
    if (!s.ok()) {
      if (exists) {
        return {Status::RedisExecErr, "TDIGEST: already exists"};
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
        return {Status::RedisExecErr, "TDIGEST: key not found"};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(conn->HeaderOfMap(9));
    output->append(redis::BulkString("Compression"));
    output->append(redis::Integer(metadata.compression));
    output->append(redis::BulkString("Capacity"));
    output->append(redis::Integer(metadata.capacity));
    output->append(redis::BulkString("Merged nodes"));
    output->append(redis::Integer(metadata.merged_nodes));
    output->append(redis::BulkString("Unmerged nodes"));
    output->append(redis::Integer(metadata.unmerged_nodes));
    output->append(redis::BulkString("Merged weight"));
    output->append(redis::Integer(metadata.merged_weight));
    output->append(redis::BulkString("Unmerged weight"));
    output->append(redis::Integer(metadata.total_weight - metadata.merged_weight));
    output->append(redis::BulkString("Observations"));
    output->append(redis::Integer(metadata.total_observations));
    output->append(redis::BulkString("Total compressions"));
    output->append(redis::Integer(metadata.merge_times));
    output->append(redis::BulkString("Memory usage"));
    output->append(redis::Integer(sizeof(TDigest)));
    return Status::OK();
  }

 private:
  std::string key_name_;
};

REDIS_REGISTER_COMMANDS(TDigest, MakeCmdAttr<CommandTDigestCreate>("tdigest.create", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandTDigestInfo>("tdigest.info", 2, "read-only", 1, 1, 1));
}  // namespace redis