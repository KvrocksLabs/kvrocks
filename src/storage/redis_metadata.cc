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

#include "redis_metadata.h"

#include <rocksdb/env.h>
#include <sys/time.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <ctime>

#include "cluster/redis_slot.h"
#include "encoding.h"
#include "time_util.h"

// 52 bit for microseconds and 11 bit for counter
const int VersionCounterBits = 11;

static std::atomic<uint64_t> version_counter_ = 0;

constexpr const char *kErrMetadataTooShort = "metadata is too short";

InternalKey::InternalKey(Slice input, bool slot_id_encoded) : slot_id_encoded_(slot_id_encoded) {
  uint32_t key_size = 0;
  uint8_t namespace_size = 0;
  GetFixed8(&input, &namespace_size);
  namespace_ = Slice(input.data(), namespace_size);
  input.remove_prefix(namespace_size);
  if (slot_id_encoded_) {
    GetFixed16(&input, &slotid_);
  }
  GetFixed32(&input, &key_size);
  key_ = Slice(input.data(), key_size);
  input.remove_prefix(key_size);
  GetFixed64(&input, &version_);
  sub_key_ = Slice(input.data(), input.size());
}

InternalKey::InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded)
    : sub_key_(sub_key), version_(version), slot_id_encoded_(slot_id_encoded) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  namespace_ = Slice(ns_key.data(), namespace_size);
  ns_key.remove_prefix(namespace_size);
  if (slot_id_encoded_) {
    GetFixed16(&ns_key, &slotid_);
  }
  key_ = ns_key;
}

Slice InternalKey::GetNamespace() const { return namespace_; }

Slice InternalKey::GetKey() const { return key_; }

Slice InternalKey::GetSubKey() const { return sub_key_; }

uint64_t InternalKey::GetVersion() const { return version_; }

std::string InternalKey::Encode() const {
  std::string out;
  size_t total = 1 + namespace_.size() + 4 + key_.size() + 8 + sub_key_.size();
  if (slot_id_encoded_) {
    total += 2;
  }
  out.resize(total);
  auto buf = out.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(namespace_.size()));
  buf = EncodeBuffer(buf, namespace_);
  if (slot_id_encoded_) {
    buf = EncodeFixed16(buf, slotid_);
  }
  buf = EncodeFixed32(buf, static_cast<uint32_t>(key_.size()));
  buf = EncodeBuffer(buf, key_);
  buf = EncodeFixed64(buf, version_);
  EncodeBuffer(buf, sub_key_);
  return out;
}

bool InternalKey::operator==(const InternalKey &that) const {
  if (namespace_ != this->namespace_) return false;
  if (key_ != that.key_) return false;
  if (sub_key_ != that.sub_key_) return false;
  return version_ == that.version_;
}

// Must slot encoded
uint16_t ExtractSlotId(Slice ns_key) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  ns_key.remove_prefix(namespace_size);

  uint16_t slot_id = HASH_SLOTS_SIZE;
  GetFixed16(&ns_key, &slot_id);
  return slot_id;
}

template <typename T>
std::tuple<T, T> ExtractNamespaceKey(Slice ns_key, bool slot_id_encoded) {
  uint8_t namespace_size = 0;
  GetFixed8(&ns_key, &namespace_size);
  T ns(ns_key.data(), namespace_size);
  ns_key.remove_prefix(namespace_size);

  if (slot_id_encoded) {
    uint16_t slot_id = 0;
    GetFixed16(&ns_key, &slot_id);
  }

  T key = {ns_key.data(), ns_key.size()};
  return {ns, key};
}

template std::tuple<Slice, Slice> ExtractNamespaceKey<Slice>(Slice ns_key, bool slot_id_encoded);
template std::tuple<std::string, std::string> ExtractNamespaceKey<std::string>(Slice ns_key, bool slot_id_encoded);

std::string ComposeNamespaceKey(const Slice &ns, const Slice &key, bool slot_id_encoded) {
  std::string ns_key;

  PutFixed8(&ns_key, static_cast<uint8_t>(ns.size()));
  ns_key.append(ns.data(), ns.size());

  if (slot_id_encoded) {
    auto slot_id = GetSlotIdFromKey(key.ToStringView());
    PutFixed16(&ns_key, slot_id);
  }

  ns_key.append(key.data(), key.size());

  return ns_key;
}

std::string ComposeSlotKeyPrefix(const Slice &ns, int slotid) {
  std::string output;

  PutFixed8(&output, static_cast<uint8_t>(ns.size()));
  output.append(ns.data(), ns.size());

  PutFixed16(&output, static_cast<uint16_t>(slotid));

  return output;
}

std::string ComposeSlotKeyUpperBound(const Slice &ns, int slotid) { return ComposeSlotKeyPrefix(ns, slotid + 1); }

Metadata::Metadata(RedisType type, bool generate_version, bool use_64bit_common_field)
    : flags((use_64bit_common_field ? METADATA_64BIT_ENCODING_MASK : 0) | (METADATA_TYPE_MASK & type)),
      expire(0),
      version(generate_version ? generateVersion() : 0),
      size(0) {}

rocksdb::Status Metadata::Decode(Slice *input) {
  if (!GetFixed8(input, &flags)) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  if (!GetExpire(input)) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  if (!IsSingleKVType()) {
    if (input->size() < 8 + CommonEncodedSize()) {
      return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
    }
    GetFixed64(input, &version);
    GetFixedCommon(input, &size);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Metadata::Decode(Slice input) { return Decode(&input); }

void Metadata::Encode(std::string *dst) const {
  PutFixed8(dst, flags);
  PutExpire(dst);
  if (!IsSingleKVType()) {
    PutFixed64(dst, version);
    PutFixedCommon(dst, size);
  }
}

void Metadata::InitVersionCounter() {
  // use random position for initial counter to avoid conflicts,
  // when the slave was promoted as master and the system clock may backoff
  version_counter_ = static_cast<uint64_t>(std::rand());
}

uint64_t Metadata::generateVersion() {
  uint64_t timestamp = util::GetTimeStampUS();
  uint64_t counter = version_counter_.fetch_add(1);
  return (timestamp << VersionCounterBits) + (counter % (1 << VersionCounterBits));
}

bool Metadata::operator==(const Metadata &that) const {
  if (flags != that.flags) return false;
  if (expire != that.expire) return false;
  if (!IsSingleKVType()) {
    if (size != that.size) return false;
    if (version != that.version) return false;
  }
  return true;
}

RedisType Metadata::Type() const { return static_cast<RedisType>(flags & METADATA_TYPE_MASK); }

size_t Metadata::GetOffsetAfterExpire(uint8_t flags) {
  if (flags & METADATA_64BIT_ENCODING_MASK) {
    return 1 + 8;
  }

  return 1 + 4;
}

size_t Metadata::GetOffsetAfterSize(uint8_t flags) {
  if (flags & METADATA_64BIT_ENCODING_MASK) {
    return 1 + 8 + 8 + 8;
  }

  return 1 + 4 + 8 + 4;
}

uint64_t Metadata::ExpireMsToS(uint64_t ms) {
  if (ms == 0) {
    return 0;
  }

  if (ms < 1000) {
    return 1;
  }

  // We use rounding to get closer to the original value
  return (ms + 499) / 1000;
}

bool Metadata::Is64BitEncoded() const { return flags & METADATA_64BIT_ENCODING_MASK; }

size_t Metadata::CommonEncodedSize() const { return Is64BitEncoded() ? 8 : 4; }

bool Metadata::GetFixedCommon(rocksdb::Slice *input, uint64_t *value) const {
  if (Is64BitEncoded()) {
    return GetFixed64(input, value);
  } else {
    uint32_t v = 0;
    bool res = GetFixed32(input, &v);
    *value = v;
    return res;
  }
}

bool Metadata::GetExpire(rocksdb::Slice *input) {
  uint64_t v = 0;

  if (!GetFixedCommon(input, &v)) {
    return false;
  }

  if (Is64BitEncoded()) {
    expire = v;
  } else {
    expire = v * 1000;
  }

  return true;
}

void Metadata::PutFixedCommon(std::string *dst, uint64_t value) const {
  if (Is64BitEncoded()) {
    PutFixed64(dst, value);
  } else {
    PutFixed32(dst, value);
  }
}

void Metadata::PutExpire(std::string *dst) const {
  if (Is64BitEncoded()) {
    PutFixed64(dst, expire);
  } else {
    PutFixed32(dst, ExpireMsToS(expire));
  }
}

int64_t Metadata::TTL() const {
  if (expire == 0) {
    return -1;
  }

  auto now = util::GetTimeStampMS();
  if (expire < now) {
    return -2;
  }

  return int64_t(expire - now);
}

timeval Metadata::Time() const {
  auto t = version >> VersionCounterBits;
  timeval created_at{static_cast<uint32_t>(t / 1000000), static_cast<int32_t>(t % 1000000)};
  return created_at;
}

bool Metadata::ExpireAt(uint64_t expired_ts) const {
  if (!IsEmptyableType() && size == 0) {
    return true;
  }
  if (expire == 0) {
    return false;
  }

  return expire < expired_ts;
}

bool Metadata::IsSingleKVType() const { return Type() == kRedisString || Type() == kRedisJson; }

bool Metadata::IsEmptyableType() const {
  return IsSingleKVType() || Type() == kRedisStream || Type() == kRedisBloomFilter || Type() == kRedisHyperLogLog ||
         Type() == kRedisTDigest;
}

bool Metadata::Expired() const { return ExpireAt(util::GetTimeStampMS()); }

ListMetadata::ListMetadata(bool generate_version)
    : Metadata(kRedisList, generate_version), head(UINT64_MAX / 2), tail(head) {}

void ListMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);
  PutFixed64(dst, head);
  PutFixed64(dst, tail);
}

rocksdb::Status ListMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < 8 + 8) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }
  GetFixed64(input, &head);
  GetFixed64(input, &tail);

  return rocksdb::Status::OK();
}

void StreamMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);

  PutFixed64(dst, last_generated_id.ms);
  PutFixed64(dst, last_generated_id.seq);

  PutFixed64(dst, recorded_first_entry_id.ms);
  PutFixed64(dst, recorded_first_entry_id.seq);

  PutFixed64(dst, max_deleted_entry_id.ms);
  PutFixed64(dst, max_deleted_entry_id.seq);

  PutFixed64(dst, first_entry_id.ms);
  PutFixed64(dst, first_entry_id.seq);

  PutFixed64(dst, last_entry_id.ms);
  PutFixed64(dst, last_entry_id.seq);

  PutFixed64(dst, entries_added);
  PutFixed64(dst, group_number);
}

rocksdb::Status StreamMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < 8 * 11) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed64(input, &last_generated_id.ms);
  GetFixed64(input, &last_generated_id.seq);

  GetFixed64(input, &recorded_first_entry_id.ms);
  GetFixed64(input, &recorded_first_entry_id.seq);

  GetFixed64(input, &max_deleted_entry_id.ms);
  GetFixed64(input, &max_deleted_entry_id.seq);

  GetFixed64(input, &first_entry_id.ms);
  GetFixed64(input, &first_entry_id.seq);

  GetFixed64(input, &last_entry_id.ms);
  GetFixed64(input, &last_entry_id.seq);

  GetFixed64(input, &entries_added);

  if (input->size() >= 8) {
    GetFixed64(input, &group_number);
  }

  return rocksdb::Status::OK();
}

void BloomChainMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);

  PutFixed16(dst, n_filters);
  PutFixed16(dst, expansion);

  PutFixed32(dst, base_capacity);
  PutDouble(dst, error_rate);
  PutFixed32(dst, bloom_bytes);
}

rocksdb::Status BloomChainMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < 20) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed16(input, &n_filters);
  GetFixed16(input, &expansion);

  GetFixed32(input, &base_capacity);
  GetDouble(input, &error_rate);
  GetFixed32(input, &bloom_bytes);

  return rocksdb::Status::OK();
}

uint32_t BloomChainMetadata::GetCapacity() const {
  // non-scaling
  if (expansion == 0) {
    return base_capacity;
  }

  // the sum of Geometric progression
  if (expansion == 1) {
    return base_capacity * n_filters;
  }
  return static_cast<uint32_t>(base_capacity * (1 - pow(expansion, n_filters)) / (1 - expansion));
}

void JsonMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);

  PutFixed8(dst, uint8_t(format));
}

rocksdb::Status JsonMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (!GetFixed8(input, reinterpret_cast<uint8_t *>(&format))) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  return rocksdb::Status::OK();
}

void HyperLogLogMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);
  PutFixed8(dst, static_cast<uint8_t>(this->encode_type));
}

rocksdb::Status HyperLogLogMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  uint8_t encoded_type = 0;
  if (!GetFixed8(input, &encoded_type)) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }
  // Check validity of encode type
  if (encoded_type > 0) {
    return rocksdb::Status::InvalidArgument(fmt::format("Invalid encode type {}", encoded_type));
  }
  this->encode_type = static_cast<EncodeType>(encoded_type);

  return rocksdb::Status::OK();
}

void TDigestMetadata::Encode(std::string *dst) const {
  Metadata::Encode(dst);
  PutFixed32(dst, compression);
  PutFixed32(dst, capacity);
  PutFixed64(dst, unmerged_nodes);
  PutFixed64(dst, merged_nodes);
  PutFixed64(dst, total_weight);
  PutFixed64(dst, merged_weight);
  PutDouble(dst, minimum);
  PutDouble(dst, maximum);
  PutFixed64(dst, total_observations);
  PutFixed64(dst, merge_times);
}

rocksdb::Status TDigestMetadata::Decode(Slice *input) {
  if (auto s = Metadata::Decode(input); !s.ok()) {
    return s;
  }

  if (input->size() < (sizeof(uint32_t) * 2 + sizeof(uint64_t) * 6 + sizeof(double) * 2)) {
    return rocksdb::Status::InvalidArgument(kErrMetadataTooShort);
  }

  GetFixed32(input, &compression);
  GetFixed32(input, &capacity);
  GetFixed64(input, &unmerged_nodes);
  GetFixed64(input, &merged_nodes);
  GetFixed64(input, &total_weight);
  GetFixed64(input, &merged_weight);
  GetDouble(input, &minimum);
  GetDouble(input, &maximum);
  GetFixed64(input, &total_observations);
  GetFixed64(input, &merge_times);

  return rocksdb::Status::OK();
}
