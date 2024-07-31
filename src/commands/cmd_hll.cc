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

#include <types/redis_hyperloglog.h>

#include <algorithm>

#include "commander.h"
#include "commands/command_parser.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

/// PFADD key [element [element ...]]
/// Complexity: O(1) for each element added.
class CommandPfAdd final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::HyperLogLog hll(srv->storage, conn->GetNamespace());
    std::vector<uint64_t> hashes(args_.size() - 1);
    for (size_t i = 1; i < args_.size(); i++) {
      hashes[i - 1] = redis::HyperLogLog::HllHash(args_[i]);
    }
    uint64_t ret{};
    auto s = hll.Add(args_[0], hashes, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// PFCOUNT key [key ...]
/// Complexity: O(1) with a very small average constant time when called with a single key.
///              O(N) with N being the number of keys, and much bigger constant times,
///              when called with multiple keys.
class CommandPfCount final : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::HyperLogLog hll(srv->storage, conn->GetNamespace());
    uint64_t ret{};
    rocksdb::Status s;
    if (args_.size() > 1) {
      std::vector<Slice> keys(args_.begin() + 1, args_.end());
      s = hll.CountMultiple(keys, &ret);
    } else {
      s = hll.Count(args_[0], &ret);
    }
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    if (s.IsNotFound()) {
      ret = 0;
    }
    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// PFMERGE destkey [sourcekey [sourcekey ...]]
///
/// complexity: O(N) to merge N HyperLogLogs, but with high constant times.
class CommandPfMerge final : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::HyperLogLog hll(srv->storage, conn->GetNamespace());
    std::vector<std::string> keys(args_.begin() + 1, args_.end());
    std::vector<Slice> dest_keys;
    dest_keys.reserve(args_.size() - 1);
    for (size_t i = 1; i < args_.size(); i++) {
      dest_keys.emplace_back(args_[i]);
    }
    auto s = hll.Merge(args_[0], dest_keys);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandPfAdd>("pfadd", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandPfCount>("pfcount", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandPfMerge>("pfmerge", -2, "write", 1, -1, 1), );

}  // namespace redis
