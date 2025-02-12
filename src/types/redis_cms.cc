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

#include "redis_cms.h"

#include <stdint.h>

#include "cms.h"
#include "rocksdb/status.h"

namespace redis {

rocksdb::Status CMS::GetMetadata(engine::Context &ctx, const Slice &ns_key, CountMinSketchMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisCountMinSketch}, ns_key, metadata);
}

rocksdb::Status CMS::IncrBy(engine::Context &ctx, const Slice &user_key, const std::vector<IncrByPair> &elements,
                            std::vector<uint32_t> *counters) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (!s.ok()) {
    // If s.NotFound() or !ok(), we should return the error directly.
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);
  counters->reserve(elements.size());
  for (const auto &element : elements) {
    // TODO(mwish): should we put the parsing result outside?
    if (element.value > 0 &&
        metadata.counter > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() - element.value)) {
      return rocksdb::Status::InvalidArgument("Overflow error: IncrBy would result in counter overflow");
    }
    uint32_t local_counter = cms.IncrBy(element.key, element.value);
    counters->push_back(local_counter);
    metadata.counter += element.value;
  }

  metadata.array = std::move(cms.GetArray());

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Info(engine::Context &ctx, const Slice &user_key, CMSketch::CMSInfo *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (!s.ok()) {
    return s;
  }

  ret->width = metadata.width;
  ret->depth = metadata.depth;
  ret->count = metadata.counter;
  return rocksdb::Status::OK();
}

rocksdb::Status CMS::InitByDim(engine::Context &ctx, const Slice &user_key, uint32_t width, uint32_t depth) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  size_t memory_used = width * depth * sizeof(uint32_t);
  // We firstly limit the memory usage to 1MB.
  constexpr size_t kMaxMemory = 1 * 1024 * 1024;

  if (memory_used == 0) {
    return rocksdb::Status::InvalidArgument("Memory usage must be greater than 0.");
  }
  if (memory_used > kMaxMemory) {
    return rocksdb::Status::InvalidArgument("Memory usage exceeds 1MB.");
  }

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);

  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("Key already exists.");
  }

  if (!s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  metadata.width = width;
  metadata.depth = depth;
  metadata.counter = 0;
  metadata.array = std::vector<uint32_t>(width * depth, 0);

  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
};

rocksdb::Status CMS::InitByProb(engine::Context &ctx, const Slice &user_key, double error, double delta) {
  if (error <= 0 || error >= 1) {
    return rocksdb::Status::InvalidArgument("Error must be between 0 and 1 (exclusive).");
  }
  if (delta <= 0 || delta >= 1) {
    return rocksdb::Status::InvalidArgument("Delta must be between 0 and 1 (exclusive).");
  }
  CMSketch::CMSketchDimensions dim = CMSketch::CMSDimFromProb(error, delta);
  return InitByDim(ctx, user_key, dim.width, dim.depth);
};

rocksdb::Status CMS::MergeUserKeys(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &src_keys,
                                   const std::vector<uint32_t> &src_weights) {
  size_t num_sources = src_keys.size();
  if (num_sources == 0) {
    return rocksdb::Status::InvalidArgument("No source keys provided for merge.");
  }
  if (src_weights.size() != num_sources) {
    return rocksdb::Status::InvalidArgument("Number of weights must match number of source keys.");
  }

  std::string dest_ns_key = AppendNamespacePrefix(user_key);
  std::vector<std::string> ns_keys{dest_ns_key};
  for (const auto &src_key : src_keys) {
    ns_keys.emplace_back(AppendNamespacePrefix(src_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), ns_keys);

  CountMinSketchMetadata dest_metadata{};
  rocksdb::Status dest_status = GetMetadata(ctx, dest_ns_key, &dest_metadata);
  if (dest_status.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("Destination CMS does not exist.");
  }
  if (!dest_status.ok()) {
    return dest_status;
  }

  CMSketch dest_cms(dest_metadata.width, dest_metadata.depth, dest_metadata.counter, dest_metadata.array);

  std::vector<CMSketch> src_cms_objects;
  src_cms_objects.reserve(num_sources);
  std::vector<uint32_t> weights_long;
  weights_long.reserve(num_sources);

  for (size_t i = 0; i < num_sources; ++i) {
    const auto &src_ns_key = ns_keys[i + 1];
    CountMinSketchMetadata src_metadata{};
    rocksdb::Status src_status = GetMetadata(ctx, src_ns_key, &src_metadata);
    if (!src_status.ok()) {
      // TODO(mwish): check the not found syntax here.
      if (src_status.IsNotFound()) {
        return rocksdb::Status::InvalidArgument("Source CMS key not found.");
      }
      return src_status;
    }

    if (src_metadata.width != dest_metadata.width || src_metadata.depth != dest_metadata.depth) {
      return rocksdb::Status::InvalidArgument("Source CMS dimensions do not match destination CMS.");
    }

    CMSketch src_cms(src_metadata.width, src_metadata.depth, src_metadata.counter, src_metadata.array);
    src_cms_objects.emplace_back(std::move(src_cms));

    weights_long.push_back(static_cast<uint32_t>(src_weights[i]));
  }
  // Initialize the destination CMS with the source CMSes after initializations
  // since vector might resize and reallocate memory.
  std::vector<const CMSketch *> src_cms_pointers(num_sources);
  for (size_t i = 0; i < num_sources; ++i) {
    src_cms_pointers[i] = &src_cms_objects[i];
  }
  auto merge_result = CMSketch::Merge(&dest_cms, num_sources, src_cms_pointers, weights_long);
  if (!merge_result.IsOK()) {
    return rocksdb::Status::InvalidArgument("Merge operation failed due to overflow or invalid dimensions.");
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisCountMinSketch);
  batch->PutLogData(log_data.Encode());

  dest_metadata.counter = dest_cms.GetCounter();
  dest_metadata.array = dest_cms.GetArray();

  std::string encoded_metadata;
  dest_metadata.Encode(&encoded_metadata);
  batch->Put(metadata_cf_handle_, dest_ns_key, encoded_metadata);

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status CMS::Query(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &elements,
                           std::vector<uint32_t> &counters) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  counters.resize(elements.size(), 0);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  CountMinSketchMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    // If s.NotFound() or !ok(), we should return the error directly.
    return s;
  }

  CMSketch cms(metadata.width, metadata.depth, metadata.counter, metadata.array);
  for (size_t i = 0; i < elements.size(); ++i) {
    counters[i] = cms.Query(elements[i]);
  }

  return rocksdb::Status::OK();
}

}  // namespace redis
