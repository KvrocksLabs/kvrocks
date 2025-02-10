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
#include <rocksdb/utilities/write_batch_with_index.h>

#include <string>
#include <vector>

#include "storage.h"

/// WriteBatchIndexer traverses the operations in WriteBatch and appends to the
/// specified WriteBatchWithIndex.
class WriteBatchIndexer : public rocksdb::WriteBatch::Handler {
 public:
  explicit WriteBatchIndexer(engine::Storage* storage, rocksdb::WriteBatchWithIndex* dest_batch,
                             const rocksdb::Snapshot* snapshot)
      : storage_(storage), dest_batch_(dest_batch), snapshot_(snapshot) {
    DCHECK_NE(storage, nullptr);
    DCHECK_NE(dest_batch, nullptr);
    DCHECK_NE(snapshot, nullptr);
  }
  explicit WriteBatchIndexer(engine::Context& ctx)
      : WriteBatchIndexer(ctx.storage, ctx.batch.get(), ctx.GetSnapshot()) {}

  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    return dest_batch_->Put(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key, value);
  }

  void Put(const rocksdb::Slice& key, const rocksdb::Slice& value) override { dest_batch_->Put(key, value); }

  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    return dest_batch_->Delete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key);
  }

  void Delete(const rocksdb::Slice& key) override { dest_batch_->Delete(key); }

  rocksdb::Status SingleDeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    return dest_batch_->SingleDelete(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key);
  }

  void SingleDelete(const rocksdb::Slice& key) override { dest_batch_->SingleDelete(key); }

  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice& begin_key,
                                const rocksdb::Slice& end_key) override {
    rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
    read_options.iterate_lower_bound = &begin_key;
    read_options.iterate_upper_bound = &end_key;
    read_options.snapshot = snapshot_;

    auto cf_handle = storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id));

    auto iter = storage_->GetDB()->NewIterator(read_options, cf_handle);
    std::unique_ptr<rocksdb::Iterator> it(dest_batch_->NewIteratorWithBase(cf_handle, iter, &read_options));
    for (it->Seek(begin_key); it->Valid() && it->key().compare(end_key) < 0; it->Next()) {
      auto s = dest_batch_->Delete(cf_handle, it->key());
      if (!s.ok()) {
        return s;
      }
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status MergeCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    return dest_batch_->Merge(storage_->GetCFHandle(static_cast<ColumnFamilyID>(column_family_id)), key, value);
  }

  void Merge(const rocksdb::Slice& key, const rocksdb::Slice& value) override { dest_batch_->Merge(key, value); }

  void LogData(const rocksdb::Slice& blob) override { dest_batch_->PutLogData(blob); }

 private:
  engine::Storage* storage_;
  rocksdb::WriteBatchWithIndex* dest_batch_;
  const rocksdb::Snapshot* snapshot_;
};
