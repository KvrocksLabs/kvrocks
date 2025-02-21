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

#include "redis_tdigest.h"

#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <range/v3/algorithm/minmax.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <vector>

#include "db_util.h"
#include "encoding.h"
#include "status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/tdigest.h"

namespace redis {

// TODO: It should be replaced by a iteration of the rocksdb iterator
class DummyCentroids {
 public:
  DummyCentroids(const TDigestMetadata& meta_data, const std::vector<Centroid>& centroids)
      : meta_data_(meta_data), centroids_(centroids) {}
  class Iterator {
   public:
    Iterator(std::vector<Centroid>::const_iterator&& iter, const std::vector<Centroid>& centroids)
        : iter_(iter), centroids_(centroids) {}
    std::unique_ptr<Iterator> Clone() const {
      if (iter_ != centroids_.cend()) {
        return std::make_unique<Iterator>(std::next(centroids_.cbegin(), std::distance(centroids_.cbegin(), iter_)),
                                          centroids_);
      }
      return std::make_unique<Iterator>(centroids_.cend(), centroids_);
    }
    bool Next() {
      if (Valid()) {
        std::advance(iter_, 1);
      }
      return iter_ != centroids_.cend();
    }

    // The Prev function can only be called for item is not cend,
    // because we must guarantee the iterator to be inside the valid range before iteration.
    bool Prev() {
      if (Valid() && iter_ != centroids_.cbegin()) {
        std::advance(iter_, -1);
      }
      return Valid();
    }
    bool Valid() const { return iter_ != centroids_.cend(); }
    StatusOr<Centroid> GetCentroid() const {
      if (iter_ == centroids_.cend()) {
        return {::Status::NotOK, "invalid iterator during decoding tdigest centroid"};
      }
      return *iter_;
    }

   private:
    std::vector<Centroid>::const_iterator iter_;
    const std::vector<Centroid>& centroids_;
  };

  std::unique_ptr<Iterator> Begin() { return std::make_unique<Iterator>(centroids_.cbegin(), centroids_); }
  std::unique_ptr<Iterator> End() {
    if (centroids_.empty()) {
      return std::make_unique<Iterator>(centroids_.cend(), centroids_);
    }
    return std::make_unique<Iterator>(std::prev(centroids_.cend()), centroids_);
  }
  double TotalWeight() const { return static_cast<double>(meta_data_.total_weight); }
  double Min() const { return meta_data_.minimum; }
  double Max() const { return meta_data_.maximum; }
  uint64_t Size() const { return meta_data_.merged_nodes; }

 private:
  const TDigestMetadata& meta_data_;
  const std::vector<Centroid>& centroids_;
};

uint32_t constexpr kMaxElements = 1 * 1024;  // 1k doubles

rocksdb::Status TDigest::Create(engine::Context& ctx, const Slice& digest_name, const TDigestCreateOptions& options,
                                bool* exists) {
  if (options.compression > kTDigestMaxCompression) {
    return rocksdb::Status::InvalidArgument(fmt::format("compression should be less than {}", kTDigestMaxCompression));
  }

  auto ns_key = AppendNamespacePrefix(digest_name);
  auto capacity = options.compression * 6 + 10;
  capacity = ((capacity < kMaxElements) ? capacity : kMaxElements);
  TDigestMetadata metadata(options.compression, capacity);

  auto status = getMetaDataByNsKey(ctx, ns_key, &metadata);
  *exists = status.ok();
  if (*exists) {
    return rocksdb::Status::InvalidArgument("tdigest already exists");
  }

  if (!status.IsNotFound()) {
    return status;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTDigest);
  if (status = batch->PutLogData(log_data.Encode()); !status.ok()) {
    return status;
  }

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  if (status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes); !status.ok()) {
    return status;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TDigest::Add(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  TDigestMetadata metadata;
  if (auto status = getMetaDataByNsKey(ctx, ns_key, &metadata); !status.ok()) {
    return status;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTDigest);
  if (auto status = batch->PutLogData(log_data.Encode()); !status.ok()) {
    return status;
  }

  metadata.total_observations += inputs.size();
  metadata.total_weight += inputs.size();

  if (metadata.unmerged_nodes + inputs.size() <= metadata.capacity) {
    if (auto status = appendBuffer(ctx, batch, ns_key, inputs, &metadata); !status.ok()) {
      return status;
    }
    metadata.unmerged_nodes += inputs.size();
  } else {
    if (auto status = mergeCurrentBuffer(ctx, ns_key, batch, &metadata, &inputs); !status.ok()) {
      return status;
    }
  }

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  if (auto status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes); !status.ok()) {
    return status;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TDigest::Quantile(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& qs,
                                  TDigestQuantitleResult* result) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  TDigestMetadata metadata;
  {
    LockGuard guard(storage_->GetLockManager(), ns_key);

    if (auto status = getMetaDataByNsKey(ctx, ns_key, &metadata); !status.ok()) {
      return status;
    }

    if (metadata.unmerged_nodes > 0) {
      auto batch = storage_->GetWriteBatchBase();
      WriteBatchLogData log_data(kRedisTDigest);
      if (auto status = batch->PutLogData(log_data.Encode()); !status.ok()) {
        return status;
      }

      if (auto status = mergeCurrentBuffer(ctx, ns_key, batch, &metadata); !status.ok()) {
        return status;
      }

      std::string metadata_bytes;
      metadata.Encode(&metadata_bytes);
      if (auto status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes); !status.ok()) {
        return status;
      }

      if (auto status = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch()); !status.ok()) {
        return status;
      }

      ctx.RefreshLatestSnapshot();
    }
  }

  std::vector<Centroid> centroids;
  if (auto status = dumpCentroids(ctx, ns_key, metadata, &centroids); !status.ok()) {
    return status;
  }

  auto dump_centroids = DummyCentroids(metadata, centroids);

  for (auto q : qs) {
    auto status_or_value = TDigestQuantile(dump_centroids, q);
    if (!status_or_value) {
      return rocksdb::Status::InvalidArgument(status_or_value.Msg());
    }
    result->quantiles.push_back(*status_or_value);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::GetMetaData(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  return Database::GetMetadata(context, {kRedisTDigest}, ns_key, metadata);
}

rocksdb::Status TDigest::getMetaDataByNsKey(engine::Context& context, const Slice& ns_key, TDigestMetadata* metadata) {
  return Database::GetMetadata(context, {kRedisTDigest}, ns_key, metadata);
}

rocksdb::Status TDigest::mergeCurrentBuffer(engine::Context& ctx, const std::string& ns_key,
                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                            TDigestMetadata* metadata, const std::vector<double>* additional_buffer) {
  std::vector<Centroid> centroids;
  std::vector<double> buffer;
  centroids.reserve(metadata->merged_nodes);
  buffer.reserve(metadata->unmerged_nodes + (additional_buffer == nullptr ? 0 : additional_buffer->size()));
  if (auto status = dumpCentroidsAndBuffer(ctx, ns_key, *metadata, &centroids, &buffer, &batch); !status.ok()) {
    return status;
  }

  if (additional_buffer != nullptr) {
    std::copy(additional_buffer->cbegin(), additional_buffer->cend(), std::back_inserter(buffer));
  }

  auto merged_centroids = TDigestMerge(buffer, {
                                                   .centroids = centroids,
                                                   .delta = metadata->compression,
                                                   .min = metadata->minimum,
                                                   .max = metadata->maximum,
                                                   .total_weight = static_cast<double>(metadata->merged_weight),
                                               });

  if (!merged_centroids.IsOK()) {
    return rocksdb::Status::InvalidArgument(merged_centroids.Msg());
  }

  if (auto status = applyNewCentroids(batch, ns_key, *metadata, merged_centroids->centroids); !status.ok()) {
    return status;
  }

  metadata->merge_times++;
  metadata->merged_nodes = merged_centroids->centroids.size();
  metadata->unmerged_nodes = 0;
  metadata->minimum = merged_centroids->min;
  metadata->maximum = merged_centroids->max;
  metadata->merged_weight = static_cast<uint64_t>(merged_centroids->total_weight);

  return rocksdb::Status::OK();
}

std::string TDigest::internalBufferKey(const std::string& ns_key, const TDigestMetadata& metadata) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kBuffer));
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TDigest::internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata,
                                             const Centroid& centroid) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kCentroids));
  PutDouble(&sub_key, centroid.mean);  // It uses EncodeDoubleToUInt64 and keeps original order of double
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TDigest::internalValueFromCentroid(const Centroid& centroid) {
  std::string value;
  PutDouble(&value, centroid.weight);
  return value;
}

rocksdb::Status TDigest::decodeCentroidFromKeyValue(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                                    Centroid* centroid) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  auto subkey = ikey.GetSubKey();
  auto type_flg = static_cast<uint8_t>(SegmentType::kGuardFlag);
  if (!GetFixed8(&subkey, &type_flg)) {
    LOG(ERROR) << "corrupted tdigest centroid key, extract type failed";
    return rocksdb::Status::Corruption("corrupted tdigest centroid key");
  }
  if (static_cast<SegmentType>(type_flg) != SegmentType::kCentroids) {
    LOG(ERROR) << "corrupted tdigest centroid key type: " << type_flg << ", expect to be "
               << static_cast<uint8_t>(SegmentType::kCentroids);
    return rocksdb::Status::Corruption("corrupted tdigest centroid key type");
  }
  if (!GetDouble(&subkey, &centroid->mean)) {
    LOG(ERROR) << "corrupted tdigest centroid key, extract mean failed";
    return rocksdb::Status::Corruption("corrupted tdigest centroid key");
  }

  if (rocksdb::Slice value_slice = value;  // GetDouble needs a mutable pointer of slice
      !GetDouble(&value_slice, &centroid->weight)) {
    LOG(ERROR) << "corrupted tdigest centroid value, extract weight failed";
    return rocksdb::Status::Corruption("corrupted tdigest centroid value");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::appendBuffer(engine::Context& ctx, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                      const std::string& ns_key, const std::vector<double>& inputs,
                                      TDigestMetadata* metadata) {
  // must guard by lock
  auto buffer_key = internalBufferKey(ns_key, *metadata);
  std::string buffer_value;
  if (auto status = storage_->Get(ctx, ctx.GetReadOptions(), cf_handle_, buffer_key, &buffer_value);
      !status.ok() && !status.IsNotFound()) {
    return status;
  }

  for (auto item : inputs) {
    PutDouble(&buffer_value, item);
  }

  if (auto status = batch->Put(cf_handle_, buffer_key, buffer_value); !status.ok()) {
    return status;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::dumpCentroidsAndBuffer(engine::Context& ctx, const std::string& ns_key,
                                                const TDigestMetadata& metadata, std::vector<Centroid>* centroids,
                                                std::vector<double>* buffer,
                                                ObserverOrUniquePtr<rocksdb::WriteBatchBase>* clean_after_dump_batch) {
  if (buffer != nullptr) {
    buffer->clear();
    buffer->reserve(metadata.unmerged_nodes);
    auto buffer_key = internalBufferKey(ns_key, metadata);
    std::string buffer_value;
    auto status = storage_->Get(ctx, ctx.GetReadOptions(), cf_handle_, buffer_key, &buffer_value);
    if (!status.ok() && !status.IsNotFound()) {
      return status;
    }

    if (status.ok()) {
      rocksdb::Slice buffer_slice = buffer_value;
      for (uint64_t i = 0; i < metadata.unmerged_nodes; ++i) {
        double tmp_value = std::numeric_limits<double>::quiet_NaN();
        if (!GetDouble(&buffer_slice, &tmp_value)) {
          LOG(ERROR) << "metadata has " << metadata.unmerged_nodes << " records, but get " << i << " failed";
          return rocksdb::Status::Corruption("corrupted tdigest buffer value");
        }
        buffer->emplace_back(tmp_value);
      }
    }

    if (clean_after_dump_batch != nullptr) {
      if (status = (*clean_after_dump_batch)->Delete(cf_handle_, buffer_key); !status.ok()) {
        return status;
      }
    }
  }

  centroids->clear();
  centroids->reserve(metadata.merged_nodes);

  auto start_key = internalSegmentGuardPrefixKey(metadata, ns_key, SegmentType::kCentroids);
  auto guard_key = internalSegmentGuardPrefixKey(metadata, ns_key, SegmentType::kGuardFlag);

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(guard_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(start_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Centroid centroid;
    if (auto status = decodeCentroidFromKeyValue(iter->key(), iter->value(), &centroid); !status.ok()) {
      return status;
    }
    centroids->emplace_back(centroid);
    if (clean_after_dump_batch != nullptr) {
      if (auto status = (*clean_after_dump_batch)->Delete(cf_handle_, iter->key()); !status.ok()) {
        return status;
      }
    }
  }

  if (centroids->size() != metadata.merged_nodes) {
    LOG(ERROR) << "metadata has " << metadata.merged_nodes << " merged nodes, but got " << centroids->size();
    return rocksdb::Status::Corruption("centroids count mismatch with metadata");
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::applyNewCentroids(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                           const std::string& ns_key, const TDigestMetadata& metadata,
                                           const std::vector<Centroid>& centroids) {
  for (const auto& c : centroids) {
    auto centroid_key = internalKeyFromCentroid(ns_key, metadata, c);
    auto centroid_payload = internalValueFromCentroid(c);
    if (auto status = batch->Put(cf_handle_, centroid_key, centroid_payload); !status.ok()) {
      return status;
    }
  }

  return rocksdb::Status::OK();
}

std::string TDigest::internalSegmentGuardPrefixKey(const TDigestMetadata& metadata, const std::string& ns_key,
                                                   SegmentType seg) const {
  std::string prefix_key;
  PutFixed8(&prefix_key, static_cast<uint8_t>(seg));
  return InternalKey(ns_key, prefix_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}
}  // namespace redis
