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

#pragma once

#include <vector>

#include "cms.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

class CMS : public Database {
 public:
  explicit CMS(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}

  struct IncrByPair {
    std::string_view key;
    int64_t value;
  };

  /// Increment the counter of the given item(s) by the specified increment(s).
  ///
  /// \param[out] counters The counter values after the increment, if the value is UINT32_MAX,
  ///                      it means the item does overflow.
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, const std::vector<IncrByPair> &elements,
                         std::vector<uint32_t> *counters);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, CMSketch::CMSInfo *ret);
  rocksdb::Status InitByDim(engine::Context &ctx, const Slice &user_key, uint32_t width, uint32_t depth);
  rocksdb::Status InitByProb(engine::Context &ctx, const Slice &user_key, double error, double delta);
  rocksdb::Status Query(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                        std::vector<uint32_t> &counters);
  rocksdb::Status MergeUserKeys(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &src_keys,
                                const std::vector<uint32_t> &src_weights);

 private:
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key,
                                            CountMinSketchMetadata *metadata);
};

}  // namespace redis
