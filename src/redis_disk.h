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

#include <string>
#include "redis_db.h"
#include "redis_metadata.h"

namespace Redis {

class Disk : public Database {
 public:
  explicit Disk(Engine::Storage *storage, const std::string &ns): Database(storage, ns) {
    this->option.include_memtabtles = true;
    this->option.include_files = true;
  }
  rocksdb::Status GetStringSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetHashSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetSetSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetListSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetZsetSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetBitmapSize(const Slice &user_key, uint64_t *key_size);
  rocksdb::Status GetSortedintSize(const Slice &user_key, uint64_t *key_size);
 private:
    rocksdb::SizeApproximationOptions option;
};

}  // namespace Redis
