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

#include "redis_hash.h"

#include <rocksdb/status.h>
#include <sys/wait.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <random>
#include <utility>

#include "db_util.h"
#include "parse_util.h"
#include "sample_helper.h"
#include "time_util.h"

namespace redis {

rocksdb::Status Hash::GetMetadata(engine::Context &ctx, const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHash}, ns_key, metadata);
}

rocksdb::Status Hash::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  *size += GetValidFieldCount(ctx, ns_key, metadata);
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(engine::Context &ctx, const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, value);
  if (!s.ok()) return s;
  uint64_t expire = 0;
  return decodeExpireFromValue(metadata, value, expire);
}

rocksdb::Status Hash::IncrBy(engine::Context &ctx, const Slice &user_key, const Slice &field, int64_t increment,
                             int64_t *new_value) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    metadata.field_encoding = HashSubkeyEncoding::VALUE_WITH_TTL;
  }

  uint64_t expire = 0;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok() && decodeExpireFromValue(metadata, &value_bytes, expire).ok()) {
      auto parse_result = ParseInt<int64_t>(value_bytes, 10);
      if (!parse_result) {
        return rocksdb::Status::InvalidArgument(parse_result.Msg());
      }
      if (isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not an integer");
      }
      old_value = *parse_result;
      exists = true;
    } else {
      // reset expire time
      expire = 0;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value)) ||
      (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *new_value = old_value + increment;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  auto value_str = std::to_string(*new_value);
  if (metadata.IsFieldExpirationEnabled()) {
    auto enc_value = encodeExpireAndValue(value_str, expire);
    batch->Put(sub_key, enc_value);
  } else {
    batch->Put(sub_key, value_str);
  }
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::IncrByFloat(engine::Context &ctx, const Slice &user_key, const Slice &field, double increment,
                                  double *new_value) {
  bool exists = false;
  double old_value = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    metadata.field_encoding = HashSubkeyEncoding::VALUE_WITH_TTL;
  }

  uint64_t expire = 0;
  std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  if (s.ok()) {
    std::string value_bytes;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok() && decodeExpireFromValue(metadata, &value_bytes, expire).ok()) {
      auto value_stat = ParseFloat(value_bytes);
      if (!value_stat || isspace(value_bytes[0])) {
        return rocksdb::Status::InvalidArgument("value is not a number");
      }
      old_value = *value_stat;
      exists = true;
    } else {
      expire = 0;
    }
  }
  double n = old_value + increment;
  if (std::isinf(n) || std::isnan(n)) {
    return rocksdb::Status::InvalidArgument("increment would produce NaN or Infinity");
  }

  *new_value = n;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  auto value_str = std::to_string(*new_value);
  if (metadata.IsFieldExpirationEnabled()) {
    auto enc_value = encodeExpireAndValue(value_str, expire);
    batch->Put(sub_key, enc_value);
  } else {
    batch->Put(sub_key, value_str);
  }
  if (!s.ok()) return s;
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                           std::vector<std::string> *values, std::vector<rocksdb::Status> *statuses) {
  values->clear();
  statuses->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  rocksdb::ReadOptions read_options = ctx.DefaultMultiGetOptions();
  std::vector<rocksdb::Slice> keys;

  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  std::vector<rocksdb::PinnableSlice> values_vector;
  values_vector.resize(keys.size());
  std::vector<rocksdb::Status> statuses_vector;
  statuses_vector.resize(keys.size());
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values_vector.data(), statuses_vector.data());

  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses_vector[i].ok() && !statuses_vector[i].IsNotFound()) return statuses_vector[i];
    auto value = values_vector[i].ToString();
    auto status = statuses_vector[i];
    if (!status.IsNotFound()) {
      uint64_t expire = 0;
      status = decodeExpireFromValue(metadata, &value, expire);
    }
    values->emplace_back(value);
    statuses->emplace_back(status);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(engine::Context &ctx, const Slice &user_key, const Slice &field, const Slice &value,
                          uint64_t *added_cnt) {
  return MSet(ctx, user_key, {{field.ToString(), value.ToString()}}, false, added_cnt);
}

rocksdb::Status Hash::Delete(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                             uint64_t *deleted_cnt) {
  *deleted_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata(false);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::unordered_set<std::string_view> field_set;
  for (const auto &field : fields) {
    if (!field_set.emplace(field.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    uint64_t expire = 0;
    if (s.ok() && decodeExpireFromValue(metadata, &value, expire).ok()) {
      *deleted_cnt += 1;
      s = batch->Delete(sub_key);
      if (!s.ok()) return s;
    }
  }
  if (*deleted_cnt == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *deleted_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::MSet(engine::Context &ctx, const Slice &user_key, const std::vector<FieldValue> &field_values,
                           bool nx, uint64_t *added_cnt, uint64_t expire) {
  *added_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  // For avoid affect existing data, we only encode ttl of field
  // on new hash object when hash_field_expiration option is yes.
  if (s.IsNotFound() && storage_->GetConfig()->hash_field_expiration) {
    metadata.field_encoding = HashSubkeyEncoding::VALUE_WITH_TTL;
  }

  bool ttl_updated = false;
  if (expire > 0 && metadata.expire != expire) {
    metadata.expire = expire;
    ttl_updated = true;
  }

  int added = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> field_set;
  for (auto it = field_values.rbegin(); it != field_values.rend(); it++) {
    if (!field_set.insert(it->field).second) {
      continue;
    }

    bool exists = false;
    std::string sub_key = InternalKey(ns_key, it->field, metadata.version, storage_->IsSlotIdEncoded()).Encode();

    uint64_t expire = 0;
    if (metadata.size > 0) {
      std::string field_value;
      s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &field_value);
      if (!s.ok() && !s.IsNotFound()) return s;

      if (s.ok()) {
        if (nx || field_value == it->value) continue;
        exists = decodeExpireFromValue(metadata, &field_value, expire).ok();
      }
    }

    if (!exists) {
      added++;
      expire = 0;
    }

    auto value = it->value;
    if (metadata.IsFieldExpirationEnabled()) {
      auto enc_value = encodeExpireAndValue(value, expire);
      s = batch->Put(sub_key, enc_value);
    } else {
      s = batch->Put(sub_key, value);
    }
    if (!s.ok()) return s;
  }

  if (added > 0 || ttl_updated) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::RangeByLex(engine::Context &ctx, const Slice &user_key, const RangeLexSpec &spec,
                                 std::vector<FieldValue> *field_values) {
  field_values->clear();
  if (spec.count == 0) {
    return rocksdb::Status::OK();
  }
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key = InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    if (spec.max_infinite) {
      iter->SeekToLast();
    } else {
      iter->SeekForPrev(start_key);
    }
  }
  int64_t pos = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); (!spec.reversed ? iter->Next() : iter->Prev())) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    if (spec.reversed) {
      if (ikey.GetSubKey().ToString() < spec.min || (spec.minex && ikey.GetSubKey().ToString() == spec.min)) {
        break;
      }
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && ikey.GetSubKey().ToString() == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && ikey.GetSubKey().ToString() == spec.max) ||
          (!spec.max_infinite && ikey.GetSubKey().ToString() > spec.max))
        break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    // filte expired field
    auto value = iter->value().ToString();
    uint64_t expire = 0;
    if (!decodeExpireFromValue(metadata, &value, expire).ok()) {
      continue;
    }
    field_values->emplace_back(ikey.GetSubKey().ToString(), value);
    if (spec.count > 0 && field_values->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::GetAll(engine::Context &ctx, const Slice &user_key, std::vector<FieldValue> *field_values,
                             HashFetchType type) {
  field_values->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    // filte expired field
    uint64_t expire = 0;
    auto value = iter->value().ToString();
    if (!decodeExpireFromValue(metadata, &value, expire).ok()) {
      continue;
    }
    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), "");
    } else if (type == HashFetchType::kOnlyValue) {
      field_values->emplace_back("", value);
    } else {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      field_values->emplace_back(ikey.GetSubKey().ToString(), value);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &field_prefix, std::vector<std::string> *fields,
                           std::vector<std::string> *values) {
  return SubKeyScanner::Scan(ctx, kRedisHash, user_key, cursor, limit, field_prefix, fields, values);
}

rocksdb::Status Hash::RandField(engine::Context &ctx, const Slice &user_key, int64_t command_count,
                                std::vector<FieldValue> *field_values, HashFetchType type) {
  uint64_t count = (command_count >= 0) ? static_cast<uint64_t>(command_count) : static_cast<uint64_t>(-command_count);
  bool unique = (command_count >= 0);

  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(/*generate_version=*/false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::vector<FieldValue> samples;
  // TODO: Getting all values in Hash might be heavy, consider lazy-loading these values later
  if (count == 0) return rocksdb::Status::OK();
  s = ExtractRandMemberFromSet<FieldValue>(
      unique, count,
      [this, user_key, type, &ctx](std::vector<FieldValue> *elements) {
        return this->GetAll(ctx, user_key, elements, type);
      },
      field_values);
  if (!s.ok()) {
    return s;
  }
  switch (type) {
    case HashFetchType::kAll:
      break;
    case HashFetchType::kOnlyKey: {
      // GetAll should only fetching the key, checking all the values is empty
      for (const FieldValue &value : *field_values) {
        DCHECK(value.value.empty());
      }
      break;
    }
    case HashFetchType::kOnlyValue:
      // Unreachable.
      DCHECK(false);
      break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::ExpireFields(engine::Context &ctx, const Slice &user_key, uint64_t expire_ms,
                                   const std::vector<Slice> &fields, HashFieldExpireType type,
                                   std::vector<int8_t> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    ret->resize(fields.size(), -2);
    return rocksdb::Status::OK();
  }

  // we don't support encode ttl on existing hash object
  if (!metadata.IsFieldExpirationEnabled()) {
    return rocksdb::Status::NotSupported(
        "can't expire fields on hash object whose field expiration feature is disabled");
  }

  std::vector<rocksdb::Slice> keys;
  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());

  // expire special field
  std::vector<rocksdb::PinnableSlice> values;
  values.resize(sub_keys.size());
  std::vector<rocksdb::Status> statuses;
  statuses.resize(sub_keys.size());

  rocksdb::ReadOptions read_options = storage_->DefaultMultiGetOptions();
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values.data(), statuses.data());

  auto now = util::GetTimeStampMS();
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses[i].ok() && !statuses[i].IsNotFound()) return statuses[i];

    // no such field exists
    if (statuses[i].IsNotFound()) {
      ret->emplace_back(-2);
      continue;
    }

    // expire with a pass time
    if (expire_ms <= now) {
      batch->Delete(sub_keys[i]);
      ret->emplace_back(2);
      metadata.size -= 1;
      continue;
    }

    auto value = values[i].ToString();
    uint64_t field_expire = 0;
    decodeExpireFromValue(metadata, &value, field_expire);

    // if a field has no associated expiration, we treated it expiration is infinite
    auto treated_expire = field_expire == 0 ? UINT64_MAX : field_expire;
    if (type == HashFieldExpireType::None || (type == HashFieldExpireType::NX && field_expire == 0) ||
        (type == HashFieldExpireType::XX && field_expire != 0) ||
        (type == HashFieldExpireType::GT && expire_ms > treated_expire) ||
        (type == HashFieldExpireType::LT && expire_ms < treated_expire)) {
      auto enc_value = encodeExpireAndValue(value, expire_ms);
      batch->Put(sub_keys[i], enc_value);
      // 1 if expiration was updated
      ret->emplace_back(1);
    } else {
      // 0 if condition has not been met
      ret->emplace_back(0);
    }
  }

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hash::PersistFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                    std::vector<int8_t> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    ret->resize(fields.size(), -2);
    return rocksdb::Status::OK();
  }
  if (!metadata.IsFieldExpirationEnabled()) {
    ret->resize(fields.size(), -1);
    return rocksdb::Status::OK();
  }

  std::vector<rocksdb::Slice> keys;
  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHash);
  batch->PutLogData(log_data.Encode());

  std::vector<rocksdb::PinnableSlice> values;
  values.resize(sub_keys.size());
  std::vector<rocksdb::Status> statuses;
  statuses.resize(sub_keys.size());
  rocksdb::ReadOptions read_options = storage_->DefaultMultiGetOptions();
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values.data(), statuses.data());

  bool removed = false;
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses[i].ok() && !statuses[i].IsNotFound()) return statuses[i];

    // no such field exists
    if (statuses[i].IsNotFound()) {
      ret->emplace_back(-2);
      continue;
    }

    auto value = values[i].ToString();
    uint64_t field_expire = 0;
    decodeExpireFromValue(metadata, &value, field_expire);
    if (field_expire == 0) {
      // -1 if the field exists but has no associated expiration
      ret->emplace_back(-1);
    } else {
      removed = true;
      auto enc_value = encodeExpireAndValue(value, 0);
      batch->Put(sub_keys[i], enc_value);
      // 1 if expiration was removed
      ret->emplace_back(1);
    }
  }

  return removed ? storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch())
                 : rocksdb::Status::OK();
}

rocksdb::Status Hash::TTLFields(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &fields,
                                std::vector<int64_t> *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  HashMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (s.IsNotFound()) {
    ret->resize(fields.size(), -2);
    return rocksdb::Status::OK();
  }

  std::vector<rocksdb::Slice> keys;
  keys.reserve(fields.size());
  std::vector<std::string> sub_keys;
  sub_keys.resize(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto &field = fields[i];
    sub_keys[i] = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    keys.emplace_back(sub_keys[i]);
  }

  std::vector<rocksdb::PinnableSlice> values;
  values.resize(sub_keys.size());
  std::vector<rocksdb::Status> statuses;
  statuses.resize(sub_keys.size());
  rocksdb::ReadOptions read_options = storage_->DefaultMultiGetOptions();
  storage_->MultiGet(ctx, read_options, storage_->GetDB()->DefaultColumnFamily(), keys.size(), keys.data(),
                     values.data(), statuses.data());

  ret->reserve(fields.size());
  auto now = util::GetTimeStampMS();
  for (size_t i = 0; i < keys.size(); i++) {
    if (!statuses[i].ok() && !statuses[i].IsNotFound()) return statuses[i];
    auto value = values[i].ToString();
    auto status = statuses[i];

    if (status.IsNotFound()) {
      ret->emplace_back(-2);
      continue;
    }

    uint64_t expire = 0;
    status = decodeExpireFromValue(metadata, &value, expire);
    if (status.IsNotFound()) {
      ret->emplace_back(-2);
    } else if (expire == 0) {
      ret->emplace_back(-1);
    } else {
      ret->emplace_back(int64_t(expire - now));
    }
  }
  return rocksdb::Status::OK();
}

bool Hash::IsFieldExpired(const Slice &metadata_key, const Slice &value) {
  HashMetadata hash_metadata(false);
  if (!hash_metadata.Decode(metadata_key).ok()) {
    return false;
  }
  if (!hash_metadata.IsFieldExpirationEnabled()) {
    return false;
  }
  uint64_t expire = 0;
  rocksdb::Slice data(value);
  GetFixed64(&data, &expire);
  return expire != 0 && expire < util::GetTimeStampMS();
}

uint64_t Hash::GetValidFieldCount(engine::Context &ctx, const Slice &ns_key, const HashMetadata &metadata) {
  if (metadata.Expired()) {
    return 0;
  }
  // if field expiration is disabled,
  // the size field in metadata is the length of hash
  if (!metadata.IsFieldExpirationEnabled()) {
    return metadata.size;
  }

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  // count the num of expired field
  uint64_t expired_field_num = 0;
  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix_key); iter->Valid() && iter->key().starts_with(prefix_key); iter->Next()) {
    uint64_t expire = 0;
    auto value = iter->value().ToString();
    if (!decodeExpireFromValue(metadata, &value, expire).ok()) {
      expired_field_num += 1;
    }
  }
  return metadata.size - expired_field_num;
}

rocksdb::Status Hash::decodeExpireFromValue(const HashMetadata &metadata, std::string *value, uint64_t &expire) {
  if (!metadata.IsFieldExpirationEnabled()) {
    return rocksdb::Status::OK();
  }
  rocksdb::Slice data(value->data(), value->size());
  GetFixed64(&data, &expire);
  *value = data.ToString();
  return (expire == 0 || expire > util::GetTimeStampMS()) ? rocksdb::Status::OK() : rocksdb::Status::NotFound();
}

std::string Hash::encodeExpireAndValue(std::string_view value, uint64_t expire) {
  std::string out;
  PutFixed64(&out, expire);
  out.append(value);
  return out;
}

}  // namespace redis
