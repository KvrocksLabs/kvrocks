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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "status.h"

namespace redis {

struct StreamEntryID {
  uint64_t ms = 0;
  uint64_t seq = 0;

  StreamEntryID() = default;
  StreamEntryID(uint64_t ms, uint64_t seq) : ms(ms), seq(seq) {}

  void Clear() {
    ms = 0;
    seq = 0;
  }

  bool IsMaximum() const { return ms == UINT64_MAX && seq == UINT64_MAX; }
  bool IsMinimum() const { return ms == 0 && seq == 0; }

  bool operator<(const StreamEntryID &rhs) const {
    if (ms < rhs.ms) return true;
    if (ms == rhs.ms) return seq < rhs.seq;
    return false;
  }

  bool operator>=(const StreamEntryID &rhs) const { return !(*this < rhs); }

  bool operator>(const StreamEntryID &rhs) const { return rhs < *this; }

  bool operator<=(const StreamEntryID &rhs) const { return !(rhs < *this); }

  bool operator==(const StreamEntryID &rhs) const { return ms == rhs.ms && seq == rhs.seq; }

  std::string ToString() const { return fmt::format("{}-{}", ms, seq); }

  static StreamEntryID Minimum() { return StreamEntryID{0, 0}; }
  static StreamEntryID Maximum() { return StreamEntryID{UINT64_MAX, UINT64_MAX}; }
};

class NextStreamEntryIDGenerationStrategy {
 public:
  virtual Status GenerateID(const StreamEntryID &last_id, StreamEntryID *next_id) = 0;
  virtual ~NextStreamEntryIDGenerationStrategy() = default;
};

class FullySpecifiedEntryID : public NextStreamEntryIDGenerationStrategy {
 public:
  explicit FullySpecifiedEntryID(StreamEntryID id) : id_(std::move(id)) {}
  ~FullySpecifiedEntryID() override = default;

  Status GenerateID(const StreamEntryID &last_id, StreamEntryID *next_id) override;

 private:
  StreamEntryID id_;
};

class AutoGeneratedEntryID : public NextStreamEntryIDGenerationStrategy {
 public:
  AutoGeneratedEntryID() = default;
  ~AutoGeneratedEntryID() override = default;

  Status GenerateID(const StreamEntryID &last_id, StreamEntryID *next_id) override;
};

class SpecificTimestampWithAnySequenceNumber : public NextStreamEntryIDGenerationStrategy {
 public:
  explicit SpecificTimestampWithAnySequenceNumber(uint64_t ms) : ms_(ms){};
  ~SpecificTimestampWithAnySequenceNumber() override = default;

  Status GenerateID(const StreamEntryID &last_id, StreamEntryID *next_id) override;

 private:
  uint64_t ms_;
};

class CurrentTimestampWithSpecificSequenceNumber : public NextStreamEntryIDGenerationStrategy {
 public:
  explicit CurrentTimestampWithSpecificSequenceNumber(uint64_t seq) : seq_(seq) {}
  ~CurrentTimestampWithSpecificSequenceNumber() override = default;

  Status GenerateID(const StreamEntryID &last_id, StreamEntryID *next_id) override;

 private:
  uint64_t seq_;
};

enum class StreamTrimStrategy {
  None = 0,
  MaxLen = 1,
  MinID = 2,
};

struct StreamEntry {
  std::string key;
  std::vector<std::string> values;

  StreamEntry(std::string k, std::vector<std::string> vv) : key(std::move(k)), values(std::move(vv)) {}
};

struct StreamTrimOptions {
  uint64_t max_len;
  StreamEntryID min_id;
  StreamTrimStrategy strategy = StreamTrimStrategy::None;
};

struct StreamAddOptions {
  StreamTrimOptions trim_options;
  std::unique_ptr<NextStreamEntryIDGenerationStrategy> next_id_strategy;
  bool nomkstream = false;
};

struct StreamRangeOptions {
  StreamEntryID start;
  StreamEntryID end;
  uint64_t count;
  bool with_count = false;
  bool reverse = false;
  bool exclude_start = false;
  bool exclude_end = false;
};

struct StreamLenOptions {
  StreamEntryID entry_id;
  bool with_entry_id = false;
  bool to_first = false;
};

struct StreamXGroupCreateOptions {
  bool mkstream = false;
  int64_t entries_read = 0;
  std::string last_id;
};

struct StreamConsumerGroupMetadata {
  uint64_t consumer_number = 0;
  uint64_t pending_number = 0;
  StreamEntryID last_delivered_id;
  int64_t entries_read = 0;
  uint64_t lag = 0;
};

struct StreamInfo {
  uint64_t size;
  uint64_t entries_added;
  StreamEntryID last_generated_id;
  StreamEntryID max_deleted_entry_id;
  StreamEntryID recorded_first_entry_id;
  std::unique_ptr<StreamEntry> first_entry;
  std::unique_ptr<StreamEntry> last_entry;
  std::vector<StreamEntry> entries;
};

struct StreamReadResult {
  std::string name;
  std::vector<StreamEntry> entries;

  StreamReadResult(std::string name, std::vector<StreamEntry> result)
      : name(std::move(name)), entries(std::move(result)) {}
};

Status IncrementStreamEntryID(StreamEntryID *id);
Status ParseStreamEntryID(const std::string &input, StreamEntryID *id);
StatusOr<std::unique_ptr<NextStreamEntryIDGenerationStrategy>> ParseNextStreamEntryIDStrategy(const std::string &input);

Status ParseRangeStart(const std::string &input, StreamEntryID *id);
Status ParseRangeEnd(const std::string &input, StreamEntryID *id);

std::string EncodeStreamEntryValue(const std::vector<std::string> &args);
Status DecodeRawStreamEntryValue(const std::string &value, std::vector<std::string> *result);

}  // namespace redis
