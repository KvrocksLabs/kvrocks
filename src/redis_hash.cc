#include "redis_hash.h"
#include <rocksdb/status.h>
#include <limits>
#include <iostream>

namespace Redis {
rocksdb::Status Hash::GetMetadata(const Slice &ns_key, HashMetadata *metadata) {
  return Database::GetMetadata(kRedisHash, ns_key, metadata);
}

rocksdb::Status Hash::Size(const Slice &user_key, uint32_t *ret) {
  *ret = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Get(const Slice &user_key, const Slice &field, std::string *value) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  if (metadata.small_hash_compress_to_meta_threshold != 0) {
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.Get(metadata, field, value);
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  return db_->Get(read_options, sub_key, value);
}

rocksdb::Status Hash::IncrBy(const Slice &user_key, const Slice &field, int64_t increment, int64_t *ret) {
  bool exists = false;
  int64_t old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.small_hash_compress_to_meta_threshold != 0
      || (metadata.size == 0 && small_hash_compress_to_meta_threshold_ > 0)) {
    if (metadata.small_hash_compress_to_meta_threshold == 0) {
      metadata.small_hash_compress_to_meta_threshold = small_hash_compress_to_meta_threshold_;
    }
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.IncrBy(&metadata, ns_key, field, increment, ret);
  }

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      try {
        old_value = std::stoll(value_bytes);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      exists = true;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN-old_value))
      || (increment > 0 && old_value > 0 && increment > (LLONG_MAX-old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::IncrByFloat(const Slice &user_key, const Slice &field, float increment, float *ret) {
  bool exists = false;
  float old_value = 0;

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.small_hash_compress_to_meta_threshold != 0
      || (metadata.size == 0 && small_hash_compress_to_meta_threshold_ > 0)) {
    if (metadata.small_hash_compress_to_meta_threshold == 0) {
      metadata.small_hash_compress_to_meta_threshold = small_hash_compress_to_meta_threshold_;
    }
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.IncrByFloat(&metadata, ns_key, field, increment, ret);
  }

  std::string sub_key;
  InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
  if (s.ok()) {
    std::string value_bytes;
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.ok()) {
      try {
        old_value = std::stof(value_bytes);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      exists = true;
    }
  }
  if ((increment < 0 && old_value < 0 && increment < (std::numeric_limits<float>::lowest()-old_value))
      || (increment > 0 && old_value > 0 && increment > (std::numeric_limits<float>::max()-old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(sub_key, std::to_string(*ret));
  if (!exists) {
    metadata.size += 1;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::MGet(const Slice &user_key,
                                const std::vector<Slice> &fields,
                                std::vector<std::string> *values) {
  values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  if (metadata.small_hash_compress_to_meta_threshold != 0) {
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.MGet(metadata, fields, values);
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
    value.clear();
    auto s = db_->Get(read_options, sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    values->emplace_back(value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Set(const Slice &user_key, const Slice &field, const Slice &value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, false, ret);
}

rocksdb::Status Hash::SetNX(const Slice &user_key, const Slice &field, Slice value, int *ret) {
  FieldValue fv = {field.ToString(), value.ToString()};
  std::vector<FieldValue> fvs;
  fvs.push_back(fv);
  return MSet(user_key, fvs, true, ret);
}

rocksdb::Status Hash::Delete(const Slice &user_key, const std::vector<Slice> &fields, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  HashMetadata metadata;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  LockGuard guard(storage_->GetLockManager(), ns_key);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.small_hash_compress_to_meta_threshold != 0) {
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.Delete(&metadata, ns_key, fields, ret);
  }

  std::string sub_key, value;
  for (const auto &field : fields) {
    InternalKey(ns_key, field, metadata.version).Encode(&sub_key);
    s = db_->Get(rocksdb::ReadOptions(), sub_key, &value);
    if (s.ok()) {
      *ret += 1;
      batch.Delete(sub_key);
    }
  }
  if (*ret == 0) {
    return rocksdb::Status::OK();
  }
  metadata.size -= *ret;
  std::string bytes;
  metadata.Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::MSet(const Slice &user_key, const std::vector<FieldValue> &field_values, bool nx, int *ret) {
  *ret = 0;
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (metadata.small_hash_compress_to_meta_threshold != 0
      || (metadata.size == 0 && small_hash_compress_to_meta_threshold_ > 0)) {
    if (metadata.small_hash_compress_to_meta_threshold == 0) {
      metadata.small_hash_compress_to_meta_threshold = small_hash_compress_to_meta_threshold_;
    }
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.MSet(&metadata, ns_key, field_values, nx, ret);
  }

  int added = 0;
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  for (const auto &fv : field_values) {
    bool exists = false;
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata.version).Encode(&sub_key);
    if (metadata.size > 0) {
      std::string fieldValue;
      s = db_->Get(rocksdb::ReadOptions(), sub_key, &fieldValue);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (((fieldValue == fv.value) || nx)) continue;
        exists = true;
      }
    }
    if (!exists) added++;
    batch.Put(sub_key, fv.value);
  }
  if (added > 0) {
    *ret = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status Hash::GetAll(const Slice &user_key, std::vector<FieldValue> *field_values, HashFetchType type) {
  field_values->clear();

  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (metadata.small_hash_compress_to_meta_threshold != 0) {
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.GetAll(metadata, field_values);
  }

  LatestSnapShot ss(db_);
  rocksdb::ReadOptions read_options;
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version).Encode(&prefix_key);
  for (iter->Seek(prefix_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    FieldValue fv;
    if (type == HashFetchType::kOnlyKey) {
      InternalKey ikey(iter->key());
      fv.field = ikey.GetSubKey().ToString();
    } else if (type == HashFetchType::kOnlyValue) {
      fv.value = iter->value().ToString();
    } else {
      InternalKey ikey(iter->key());
      fv.field = ikey.GetSubKey().ToString();
      fv.value = iter->value().ToString();
    }
    field_values->emplace_back(fv);
  }
  delete iter;
  return rocksdb::Status::OK();
}

rocksdb::Status Hash::Scan(const Slice &user_key,
                                const std::string &cursor,
                                uint64_t limit,
                                const std::string &field_prefix,
                                std::vector<std::string> *fields) {
  std::string ns_key;
  AppendNamespacePrefix(user_key, &ns_key);
  HashMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  if (metadata.small_hash_compress_to_meta_threshold != 0) {
    Redis::HashCompressed hash_compressed_db(storage_, namespace_);
    return hash_compressed_db.Scan(metadata, cursor, limit, field_prefix, fields);
  }

  return SubKeyScanner::Scan(kRedisHash, user_key, cursor, limit, field_prefix, fields);
}

}  // namespace Redis
