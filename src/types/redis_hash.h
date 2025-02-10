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

#include <rocksdb/status.h>

#include <string>
#include <vector>

#include "common/range_spec.h"
#include "encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

struct FieldValue {
  std::string field;
  std::string value;

  FieldValue(std::string f, std::string v) : field(std::move(f)), value(std::move(v)) {}
};

enum class HashFetchType { kAll = 0, kOnlyKey = 1, kOnlyValue = 2 };

enum class HashFieldExpireType { None, NX, XX, GT, LT };

namespace redis {

class Hash : public SubKeyScanner {
 public:
  Hash(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

  rocksdb::Status Size(engine::Context &ctx, const Slice &user_key, uint64_t *size);
  rocksdb::Status Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value);
  rocksdb::Status Set(engine::Context &ctx, const Slice &user_key, const Slice &field, const Slice &value,
                      uint64_t *added_cnt);
  rocksdb::Status Delete(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                         uint64_t *deleted_cnt);
  rocksdb::Status IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                         int64_t *new_value);
  rocksdb::Status IncrByFloat(engine::Context &ctx, const Slice &user_key, const Slice &field, double increment,
                              double *new_value);
  rocksdb::Status MSet(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                       bool nx, uint64_t *added_cnt, uint64_t expire = 0);
  rocksdb::Status RangeByLex(engine::Context &ctx, const Slice &user_key, const RangeLexSpec &spec,
                             std::vector<FieldValue> *field_values);
  rocksdb::Status MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                       std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses);
  rocksdb::Status GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                         HashFetchType type = HashFetchType::kAll);
  rocksdb::Status Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                       const std::string &field_prefix, std::vector<std::string> *fields,
                       std::vector<std::string> *values = nullptr);
  rocksdb::Status RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                            std::vector<FieldValue> *field_values, HashFetchType type = HashFetchType::kOnlyKey);
  rocksdb::Status ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expire_ms,
                               const std::vector<Slice> &fields, HashFieldExpireType type, std::vector<int8_t> *ret);
  rocksdb::Status PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                std::vector<int8_t> *ret);
  rocksdb::Status TTLFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                            std::vector<int64_t> *ret);
  uint64_t GetValidFieldCount(engine::Context &ctx, const Slice &ns_key, const HashMetadata &metadata);
  static bool IsFieldExpired(const Slice &metadata_key, const Slice &value);

 private:
  rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata);
  static rocksdb::Status decodeExpireFromValue(const HashMetadata &metadata, std::string *value, uint64_t &expire);
  static std::string encodeExpireAndValue(std::string_view value, uint64_t expire);

  friend struct FieldValueRetriever;
};

}  // namespace redis
