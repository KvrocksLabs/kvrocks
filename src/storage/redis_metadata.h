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
#include <sys/time.h>

#include <atomic>
#include <bitset>
#include <initializer_list>
#include <limits>
#include <string>
#include <vector>

#include "encoding.h"
#include "types/redis_stream_base.h"

constexpr bool USE_64BIT_COMMON_FIELD_DEFAULT = METADATA_ENCODING_VERSION != 0;

// We write enum integer value of every datatype
// explicitly since it cannot be changed once confirmed
// Note that if you want to add a new redis type in `RedisType`
// you should also add a type name to the `RedisTypeNames` below
enum RedisType : uint8_t {
  kRedisNone = 0,
  kRedisString = 1,
  kRedisHash = 2,
  kRedisList = 3,
  kRedisSet = 4,
  kRedisZSet = 5,
  kRedisBitmap = 6,
  kRedisSortedint = 7,
  kRedisStream = 8,
  kRedisBloomFilter = 9,
  kRedisJson = 10,
  kRedisHyperLogLog = 11,
  kRedisTDigest = 12,
};

struct RedisTypes {
  RedisTypes(std::initializer_list<RedisType> list) {
    for (auto type : list) {
      types_.set(type);
    }
  }

  static RedisTypes All() {
    UnderlyingType types;
    types.set();
    return RedisTypes(types);
  }

  bool Contains(RedisType type) const { return types_[type]; }

 private:
  using UnderlyingType = std::bitset<128>;

  explicit RedisTypes(UnderlyingType types) : types_(types) {}

  UnderlyingType types_;
};

enum RedisCommand {
  kRedisCmdLSet,
  kRedisCmdLInsert,
  kRedisCmdLTrim,
  kRedisCmdLPop,
  kRedisCmdRPop,
  kRedisCmdLRem,
  kRedisCmdLPush,
  kRedisCmdRPush,
  kRedisCmdExpire,
  kRedisCmdSetBit,
  kRedisCmdBitOp,
  kRedisCmdBitfield,
  kRedisCmdLMove,
};

const std::vector<std::string> RedisTypeNames = {"none",      "string",      "hash",      "list",   "set",
                                                 "zset",      "bitmap",      "sortedint", "stream", "MBbloom--",
                                                 "ReJSON-RL", "hyperloglog", "TDIS-TYPE"};

constexpr const char *kErrMsgWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";
constexpr const char *kErrMsgKeyExpired = "the key was expired";

using rocksdb::Slice;

struct KeyNumStats {
  uint64_t n_key = 0;
  uint64_t n_expires = 0;
  uint64_t n_expired = 0;
  uint64_t avg_ttl = 0;
};

[[nodiscard]] uint16_t ExtractSlotId(Slice ns_key);
template <typename T = Slice>
[[nodiscard]] std::tuple<T, T> ExtractNamespaceKey(Slice ns_key, bool slot_id_encoded);
[[nodiscard]] std::string ComposeNamespaceKey(const Slice &ns, const Slice &key, bool slot_id_encoded);
[[nodiscard]] std::string ComposeSlotKeyPrefix(const Slice &ns, int slotid);
[[nodiscard]] std::string ComposeSlotKeyUpperBound(const Slice &ns, int slotid);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version, bool slot_id_encoded);
  explicit InternalKey(Slice input, bool slot_id_encoded);
  ~InternalKey() = default;

  Slice GetNamespace() const;
  Slice GetKey() const;
  Slice GetSubKey() const;
  uint64_t GetVersion() const;
  [[nodiscard]] std::string Encode() const;
  bool operator==(const InternalKey &that) const;

 private:
  Slice namespace_;
  Slice key_;
  Slice sub_key_;
  uint64_t version_;
  uint16_t slotid_;
  bool slot_id_encoded_;
};

constexpr uint8_t METADATA_64BIT_ENCODING_MASK = 0x80;
constexpr uint8_t METADATA_TYPE_MASK = 0x0f;

class Metadata {
 public:
  // metadata flags
  // <(1-bit) 64bit-common-field-indicator> 0 0 0 <(4-bit) redis-type>
  // 64bit-common-field-indicator: make `expire` and `size` 64bit instead of 32bit
  // NOTE: `expire` is stored in milliseconds for 64bit, seconds for 32bit
  // redis-type: RedisType for the key-value
  uint8_t flags;

  // expire timestamp, in milliseconds
  uint64_t expire;

  // the current version: 53bit timestamp + 11bit counter
  uint64_t version;

  // element size of the key-value
  uint64_t size;

  explicit Metadata(RedisType type, bool generate_version = true,
                    bool use_64bit_common_field = USE_64BIT_COMMON_FIELD_DEFAULT);

  static void InitVersionCounter();

  static size_t GetOffsetAfterExpire(uint8_t flags);
  static size_t GetOffsetAfterSize(uint8_t flags);
  static uint64_t ExpireMsToS(uint64_t ms);

  bool Is64BitEncoded() const;
  bool GetFixedCommon(rocksdb::Slice *input, uint64_t *value) const;
  bool GetExpire(rocksdb::Slice *input);
  void PutFixedCommon(std::string *dst, uint64_t value) const;
  void PutExpire(std::string *dst) const;

  RedisType Type() const;
  size_t CommonEncodedSize() const;
  int64_t TTL() const;
  timeval Time() const;
  bool Expired() const;
  bool ExpireAt(uint64_t expired_ts) const;

  // return whether for this type, the metadata itself is the whole data,
  // no other key-values.
  // this means that the metadata of these types do NOT have
  // `version` and `size` field.
  // e.g. RedisString, RedisJson
  bool IsSingleKVType() const;

  // return whether the `size` field of this type can be zero.
  // if a type is NOT an emptyable type,
  // any key of this type is regarded as expired if `size` equals to 0.
  // e.g. any SingleKVType, RedisStream, RedisBloomFilter
  bool IsEmptyableType() const;

  virtual void Encode(std::string *dst) const;
  [[nodiscard]] virtual rocksdb::Status Decode(Slice *input);
  [[nodiscard]] rocksdb::Status Decode(Slice input);

  bool operator==(const Metadata &that) const;
  virtual ~Metadata() = default;

 private:
  static uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  explicit HashMetadata(bool generate_version = true) : Metadata(kRedisHash, generate_version) {}
};

class SetMetadata : public Metadata {
 public:
  explicit SetMetadata(bool generate_version = true) : Metadata(kRedisSet, generate_version) {}
};

class ZSetMetadata : public Metadata {
 public:
  explicit ZSetMetadata(bool generate_version = true) : Metadata(kRedisZSet, generate_version) {}
};

class BitmapMetadata : public Metadata {
 public:
  explicit BitmapMetadata(bool generate_version = true) : Metadata(kRedisBitmap, generate_version) {}
};

class SortedintMetadata : public Metadata {
 public:
  explicit SortedintMetadata(bool generate_version = true) : Metadata(kRedisSortedint, generate_version) {}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  explicit ListMetadata(bool generate_version = true);

  void Encode(std::string *dst) const override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *input) override;
};

class StreamMetadata : public Metadata {
 public:
  redis::StreamEntryID last_generated_id;
  redis::StreamEntryID recorded_first_entry_id;
  redis::StreamEntryID max_deleted_entry_id;
  redis::StreamEntryID first_entry_id;
  redis::StreamEntryID last_entry_id;
  uint64_t entries_added = 0;
  uint64_t group_number = 0;

  explicit StreamMetadata(bool generate_version = true) : Metadata(kRedisStream, generate_version) {}

  void Encode(std::string *dst) const override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *input) override;
};

class BloomChainMetadata : public Metadata {
 public:
  /// The number of sub-filters
  uint16_t n_filters;

  /// Adding an element to a Bloom filter never fails due to the data structure "filling up". Instead the error rate
  /// starts to grow. To keep the error close to the one set on filter initialisation - the bloom filter will
  /// auto-scale, meaning when capacity is reached an additional sub-filter will be created.
  ///
  /// The capacity of the new sub-filter is the capacity of the last sub-filter multiplied by expansion.
  ///
  /// The default expansion value is 2.
  ///
  /// For non-scaling, expansion should be set to 0
  uint16_t expansion;

  /// The number of entries intended to be added to the filter. If your filter allows scaling, the capacity of the last
  /// sub-filter should be: base_capacity -> base_capacity * expansion -> base_capacity * expansion^2...
  ///
  /// The default base_capacity value is 100.
  uint32_t base_capacity;

  /// The desired probability for false positives.
  ///
  /// The rate is a decimal value between 0 and 1. For example, for a desired false positive rate of 0.1% (1 in 1000),
  /// error_rate should be set to 0.001.
  ///
  /// The default error_rate value is 0.01.
  double error_rate;

  /// The total number of bytes allocated for all sub-filters.
  uint32_t bloom_bytes;

  explicit BloomChainMetadata(bool generate_version = true) : Metadata(kRedisBloomFilter, generate_version) {}

  void Encode(std::string *dst) const override;
  using Metadata::Decode;
  rocksdb::Status Decode(Slice *bytes) override;

  uint32_t GetCapacity() const;

  bool IsScaling() const { return expansion != 0; };
};

enum class JsonStorageFormat : uint8_t {
  JSON = 0,
  CBOR = 1,
};

class JsonMetadata : public Metadata {
 public:
  // to make JSON type more extensible,
  // we add a field to indicate the format of stored data
  JsonStorageFormat format = JsonStorageFormat::JSON;

  explicit JsonMetadata(bool generate_version = true) : Metadata(kRedisJson, generate_version) {}

  void Encode(std::string *dst) const override;
  rocksdb::Status Decode(Slice *input) override;
};

class HyperLogLogMetadata : public Metadata {
 public:
  enum class EncodeType : uint8_t {
    // Redis-style dense encoding implement as bitmap like sub keys to
    // store registers by segment in data column family.
    // The registers are stored in 6-bit format and each segment contains
    // 768 registers.
    DENSE = 0,
    // TODO(mwish): sparse encoding
    // SPARSE = 1,
  };

  explicit HyperLogLogMetadata(bool generate_version = true) : Metadata(kRedisHyperLogLog, generate_version) {}

  void Encode(std::string *dst) const override;
  rocksdb::Status Decode(Slice *input) override;

  EncodeType encode_type = EncodeType::DENSE;
};

class TDigestMetadata : public Metadata {
 public:
  uint32_t compression;
  uint32_t capacity;
  uint64_t unmerged_nodes = 0;
  uint64_t merged_nodes = 0;
  uint64_t total_weight = 0;
  uint64_t merged_weight = 0;
  double minimum = std::numeric_limits<double>::max();
  double maximum = std::numeric_limits<double>::lowest();
  uint64_t total_observations = 0;  // reserved for TDIGEST.INFO command
  uint64_t merge_times = 0;         // reserved for TDIGEST.INFO command

  explicit TDigestMetadata(uint32_t compression, uint32_t capacity, bool generate_version = true)
      : Metadata(kRedisTDigest, generate_version), compression(compression), capacity(capacity) {}
  explicit TDigestMetadata(bool generate_version = true) : TDigestMetadata(0, 0, generate_version) {}
  void Encode(std::string *dst) const override;
  rocksdb::Status Decode(Slice *input) override;

  uint64_t TotalNodes() const { return merged_nodes + unmerged_nodes; }

  double Delta() const { return 1. / static_cast<double>(compression); }
};
