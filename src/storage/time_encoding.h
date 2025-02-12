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

#include <encoding.h>
#include <storage/redis_metadata.h>

#include <memory>

namespace redis {

enum class TimeSeriesSubkeyType : uint8_t {
  METADATA = 0,
  DATA_POINT = 1,
  LABEL_INDEX = 2,
  AGGREGATION_RULE = 3,
};

enum class DuplicatePolicy : uint8_t {
  BLOCK = 0,
  FIRST = 1,
  LAST = 2,
  MIN = 3,
  MAX = 4,
  SUM = 5,
};

class TimeSeriesMetadata : public Metadata {
 public:
  uint64_t num_samples = 0;
  uint64_t total_memory = 0;
  uint64_t first_ts = 0;
  uint64_t last_ts = 0;
  uint64_t retention_time = 0;  // In milliseconds
  uint32_t chunk_count = 0;
  uint32_t chunk_size = 4096;  // Default chunk size
  uint8_t chunk_type = 1;      // Compressed = 1, Decompressed = 0
  DuplicatePolicy duplicate_policy = DuplicatePolicy::BLOCK;
  std::string source_key;  // For aggregations

  explicit TimeSeriesMetadata(bool generate_version = true) : Metadata(kRedisTimeSeries, generate_version) {}

  void Encode(std::string *dst) const override {
    Metadata::Encode(dst);
    PutFixed64(dst, num_samples);
    PutFixed64(dst, total_memory);
    PutFixed64(dst, first_ts);
    PutFixed64(dst, last_ts);
    PutFixed64(dst, retention_time);
    PutFixed32(dst, chunk_count);
    PutFixed32(dst, chunk_size);
    PutFixed8(dst, chunk_type);
    PutFixed8(dst, uint8_t(duplicate_policy));
    PutSizedString(dst, source_key);
  }

  rocksdb::Status Decode(Slice *input) override {
    rocksdb::Status s = Metadata::Decode(input);
    GetFixed64(input, &num_samples);
    GetFixed64(input, &total_memory);
    GetFixed64(input, &first_ts);
    GetFixed64(input, &last_ts);
    GetFixed64(input, &retention_time);
    GetFixed32(input, &chunk_count);
    GetFixed32(input, &chunk_size);
    GetFixed8(input, &chunk_type);
    uint8_t dp{};
    GetFixed8(input, &dp);
    duplicate_policy = static_cast<DuplicatePolicy>(dp);

    rocksdb::Slice source_key_slice;
    if (!GetSizedString(input, &source_key_slice)) {
      return rocksdb::Status::Corruption("Failed to decode source_key");
    }
    source_key = std::string(source_key_slice.data(), source_key_slice.size());
    return rocksdb::Status::OK();
  }
};

struct TimeSeriesKey {
  std::string_view ns;
  std::string_view key;
  uint64_t version = 0;
  uint64_t timestamp = 0;
  TimeSeriesSubkeyType subkey_type;

  TimeSeriesKey(std::string_view ns, std::string_view key, TimeSeriesSubkeyType subkey_type)
      : ns(ns), key(key), subkey_type(subkey_type) {}

  TimeSeriesKey(std::string_view ns, std::string_view key, uint64_t version, uint64_t timestamp)
      : ns(ns), key(key), version(version), timestamp(timestamp), subkey_type(TimeSeriesSubkeyType::DATA_POINT) {}

  void PutNamespace(std::string *dst) const {
    PutFixed8(dst, ns.size());
    dst->append(ns);
  }

  void PutType(std::string *dst) const { PutFixed8(dst, uint8_t(subkey_type)); }

  void PutKey(std::string *dst) const { PutSizedString(dst, key); }

  std::string ConstructDataPointKey() const {
    std::string dst;
    PutNamespace(&dst);
    PutType(&dst);
    PutKey(&dst);
    PutFixed64(&dst, version);
    PutFixed64(&dst, timestamp);
    return dst;
  }

  std::string ConstructLabelIndexKey(const std::string &label_key, const std::string &label_value) const {
    std::string dst;
    PutNamespace(&dst);
    PutFixed8(&dst, uint8_t(TimeSeriesSubkeyType::LABEL_INDEX));
    PutSizedString(&dst, label_key);
    PutSizedString(&dst, label_value);
    PutKey(&dst);
    return dst;
  }

  std::string ConstructAggregationRuleKey(const std::string &dest_key) const {
    std::string dst;
    PutNamespace(&dst);
    PutFixed8(&dst, uint8_t(TimeSeriesSubkeyType::AGGREGATION_RULE));
    PutKey(&dst);
    PutFixed64(&dst, version);
    PutSizedString(&dst, dest_key);
    return dst;
  }
};

enum class AggregationRules : uint8_t {
  avg = 0,
  sum,
  min,
  max,
};

struct AggregationIndex {
  uint64_t bucket_duration;
  AggregationRules aggregator;

  void Encode(std::string *dst) const {
    PutFixed64(dst, bucket_duration);
    PutFixed8(dst, uint8_t(aggregator));
  }

  rocksdb::Status Decode(Slice *input) {
    GetFixed64(input, &bucket_duration);
    uint8_t aggregator = 0;
    GetFixed8(input, &aggregator);
    this->aggregator = static_cast<AggregationRules>(aggregator);
    return rocksdb::Status::OK();
  }
};
}  // namespace redis
