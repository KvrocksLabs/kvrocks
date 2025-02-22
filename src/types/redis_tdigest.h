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

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "tdigest.h"

namespace redis {
inline constexpr uint32_t kTDigestMaxCompression = 1000;  // limit the compression to 1k

struct CentroidWithKey {
  Centroid centroid;
  rocksdb::Slice key;
};

struct TDigestCreateOptions {
  uint32_t compression;
};

struct TDigestQuantitleResult {
  std::vector<double> quantiles;
};

class TDigest : public SubKeyScanner {
 public:
  using Slice = rocksdb::Slice;
  explicit TDigest(engine::Storage* storage, const std::string& ns)
      : SubKeyScanner(storage, ns), cf_handle_(storage->GetCFHandle(ColumnFamilyID::PrimarySubkey)) {}
  /**
   * @brief Create a t-digest structure.
   *
   * @param ctx The context of the operation.
   * @param digest_name The name of the t-digest.
   * @param options The options of the t-digest.
   * @param exists The output parameter to indicate whether the t-digest already exists.
   * @return rocksdb::Status
   */
  rocksdb::Status Create(engine::Context& ctx, const Slice& digest_name, const TDigestCreateOptions& options,
                         bool* exists);
  rocksdb::Status Add(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs);
  rocksdb::Status Quantile(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& qs,
                           TDigestQuantitleResult* result);

  rocksdb::Status GetMetaData(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);

 private:
  enum class SegmentType : uint8_t { kBuffer = 0, kCentroids = 1, kGuardFlag = 0xFF };

  rocksdb::ColumnFamilyHandle* cf_handle_;

  rocksdb::Status getMetaDataByNsKey(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);

  rocksdb::Status appendBuffer(engine::Context& ctx, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                               const std::string& ns_key, const std::vector<double>& inputs, TDigestMetadata* metadata);

  rocksdb::Status dumpCentroids(engine::Context& ctx, const std::string& ns_key, const TDigestMetadata& metadata,
                                std::vector<Centroid>* centroids) {
    return dumpCentroidsAndBuffer(ctx, ns_key, metadata, centroids, nullptr, nullptr);
  }

  /**
   * @brief Dumps the centroids and buffer of the t-digest.
   *
   * This function reads the centroids and buffer from persistent storage and removes them from the storage.
   * @param ctx The context of the operation.
   * @param ns_key The namespace key of the t-digest.
   * @param metadata The metadata of the t-digest.
   * @param centroids The output vector to store the centroids.
   * @param buffer The output vector to store the buffer. If it is nullptr, the buffer will not be read.
   * @param clean_after_dump_batch The write batch to store the clean operations. If it is nullptr, the clean operations
   * @return rocksdb::Status
   */
  rocksdb::Status dumpCentroidsAndBuffer(engine::Context& ctx, const std::string& ns_key,
                                         const TDigestMetadata& metadata, std::vector<Centroid>* centroids,
                                         std::vector<double>* buffer,
                                         ObserverOrUniquePtr<rocksdb::WriteBatchBase>* clean_after_dump_batch);
  rocksdb::Status applyNewCentroids(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, const std::string& ns_key,
                                    const TDigestMetadata& metadata, const std::vector<Centroid>& centroids);

  std::string internalSegmentGuardPrefixKey(const TDigestMetadata& metadata, const std::string& ns_key,
                                            SegmentType seg) const;

  rocksdb::Status mergeCurrentBuffer(engine::Context& ctx, const std::string& ns_key,
                                     ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, TDigestMetadata* metadata,
                                     const std::vector<double>* additional_buffer = nullptr);
  std::string internalBufferKey(const std::string& ns_key, const TDigestMetadata& metadata) const;
  std::string internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata,
                                      const Centroid& centroid) const;
  static std::string internalValueFromCentroid(const Centroid& centroid);
  rocksdb::Status decodeCentroidFromKeyValue(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                             Centroid* centroid) const;
};

}  // namespace redis
