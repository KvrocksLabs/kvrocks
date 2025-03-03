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

#include "indexer.h"

#include <algorithm>
#include <variant>

#include "db_util.h"
#include "parse_util.h"
#include "search/hnsw_indexer.h"
#include "search/search_encoding.h"
#include "search/value.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "string_util.h"
#include "types/redis_hash.h"

namespace redis {

StatusOr<FieldValueRetriever> FieldValueRetriever::Create(IndexOnDataType type, std::string_view key,
                                                          engine::Storage *storage, const std::string &ns) {
  engine::Context ctx(storage);
  if (type == IndexOnDataType::HASH) {
    Hash db(storage, ns);
    std::string ns_key = db.AppendNamespacePrefix(key);
    HashMetadata metadata(false);

    auto s = db.GetMetadata(ctx, ns_key, &metadata);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
    return FieldValueRetriever(db, metadata, key);
  } else if (type == IndexOnDataType::JSON) {
    Json db(storage, ns);
    std::string ns_key = db.AppendNamespacePrefix(key);
    JsonMetadata metadata(false);
    JsonValue value;
    auto s = db.read(ctx, ns_key, &metadata, &value);
    if (!s.ok()) return {Status::NotOK, s.ToString()};
    return FieldValueRetriever(value);
  } else {
    assert(false && "unreachable code: unexpected IndexOnDataType");
    __builtin_unreachable();
  }
}

StatusOr<kqir::Value> FieldValueRetriever::ParseFromJson(const jsoncons::json &val,
                                                         const redis::IndexFieldMetadata *type) {
  if (auto numeric [[maybe_unused]] = dynamic_cast<const redis::NumericFieldMetadata *>(type)) {
    if (!val.is_number() || val.is_string()) return {Status::NotOK, "json value cannot be string for numeric fields"};
    return kqir::MakeValue<kqir::Numeric>(val.as_double());
  } else if (auto tag = dynamic_cast<const redis::TagFieldMetadata *>(type)) {
    if (val.is_string()) {
      const char delim[] = {tag->separator, '\0'};
      auto vec = util::Split(val.as_string(), delim);
      std::transform(vec.begin(), vec.end(), vec.begin(),
                     [](const std::string &s) { return util::Trim(s, util::ASCII_WHITESPACES); });
      return kqir::MakeValue<kqir::StringArray>(vec);
    } else if (val.is_array()) {
      std::vector<std::string> strs;
      for (size_t i = 0; i < val.size(); ++i) {
        if (!val[i].is_string())
          return {Status::NotOK, "json value should be string or array of strings for tag fields"};
        strs.push_back(val[i].as_string());
      }
      return kqir::MakeValue<kqir::StringArray>(strs);
    } else {
      return {Status::NotOK, "json value should be string or array of strings for tag fields"};
    }
  } else if (auto vector = dynamic_cast<const redis::HnswVectorFieldMetadata *>(type)) {
    const auto dim = vector->dim;
    if (!val.is_array()) return {Status::NotOK, "json value should be array of numbers for vector fields"};
    if (dim != val.size()) return {Status::NotOK, "the size of the json array is not equal to the dim of the vector"};
    std::vector<double> nums;
    for (size_t i = 0; i < dim; ++i) {
      if (!val[i].is_number() || val[i].is_string())
        return {Status::NotOK, "json value should be array of numbers for vector fields"};
      nums.push_back(val[i].as_double());
    }
    return kqir::MakeValue<kqir::NumericArray>(nums);
  } else if ([[maybe_unused]] auto text = dynamic_cast<const redis::TextFieldMetadata *>(type)) {
    return kqir::MakeValue<kqir::String>(val.as_string());
  } else {
    return {Status::NotOK, "unknown field type to retrieve"};
  }
}

StatusOr<kqir::Value> FieldValueRetriever::ParseFromHash(const std::string &value,
                                                         const redis::IndexFieldMetadata *type) {
  if (auto numeric [[maybe_unused]] = dynamic_cast<const redis::NumericFieldMetadata *>(type)) {
    auto num = GET_OR_RET(ParseFloat(value));
    return kqir::MakeValue<kqir::Numeric>(num);
  } else if (auto tag = dynamic_cast<const redis::TagFieldMetadata *>(type)) {
    const char delim[] = {tag->separator, '\0'};
    auto vec = util::Split(value, delim);
    std::transform(vec.begin(), vec.end(), vec.begin(),
                   [](const std::string &s) { return util::Trim(s, util::ASCII_WHITESPACES); });
    return kqir::MakeValue<kqir::StringArray>(vec);
  } else if (auto vector = dynamic_cast<const redis::HnswVectorFieldMetadata *>(type)) {
    const auto dim = vector->dim;
    if (value.size() != dim * sizeof(double)) {
      return {Status::NotOK, "field value is too short or too long to be parsed as a vector"};
    }
    std::vector<double> vec;
    for (size_t i = 0; i < dim; ++i) {
      // TODO: care about endian later
      // TODO: currently only support 64bit floating point
      vec.push_back(*(reinterpret_cast<const double *>(value.data()) + i));
    }
    return kqir::MakeValue<kqir::NumericArray>(vec);
  } else if ([[maybe_unused]] auto text = dynamic_cast<const redis::TextFieldMetadata *>(type)) {
    return kqir::MakeValue<kqir::String>(value);
  } else {
    return {Status::NotOK, "unknown field type to retrieve"};
  }
}

StatusOr<kqir::Value> FieldValueRetriever::Retrieve(engine::Context &ctx, std::string_view field,
                                                    const redis::IndexFieldMetadata *type) {
  if (std::holds_alternative<HashData>(db)) {
    auto &[hash, metadata, key] = std::get<HashData>(db);
    std::string ns_key = hash.AppendNamespacePrefix(key);
    std::string sub_key = InternalKey(ns_key, field, metadata.version, hash.storage_->IsSlotIdEncoded()).Encode();
    std::string value;
    auto s = hash.storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.IsNotFound()) return {Status::NotFound, s.ToString()};
    if (!s.ok()) return {Status::NotOK, s.ToString()};

    return ParseFromHash(value, type);
  } else if (std::holds_alternative<JsonData>(db)) {
    auto &value = std::get<JsonData>(db);

    auto s = value.Get(field.front() == '$' ? field : fmt::format("$.{}", field));
    if (!s.IsOK()) return {Status::NotOK, s.Msg()};
    if (s->value.size() != 1)
      return {Status::NotFound, "json value specified by the field (json path) should exist and be unique"};
    auto val = s->value[0];

    return ParseFromJson(val, type);
  } else {
    return {Status::NotOK, "unknown redis data type to retrieve"};
  }
}

StatusOr<IndexUpdater::FieldValues> IndexUpdater::Record(engine::Context &ctx, std::string_view key) const {
  const auto &ns = info->ns;
  Database db(indexer->storage, ns);

  RedisType type = kRedisNone;
  auto s = db.Type(ctx, key, &type);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  // key not exist
  if (type == kRedisNone) return FieldValues();

  if (type != static_cast<RedisType>(info->metadata.on_data_type)) {
    // not the expected type, stop record
    return {Status::TypeMismatched};
  }

  auto retriever = GET_OR_RET(FieldValueRetriever::Create(info->metadata.on_data_type, key, indexer->storage, ns));

  FieldValues values;
  for (const auto &[field, i] : info->fields) {
    if (i.metadata->noindex) {
      continue;
    }

    auto s = retriever.Retrieve(ctx, field, i.metadata.get());
    if (s.Is<Status::NotFound>()) continue;
    if (!s) return s;

    values.emplace(field, *s);
  }

  return values;
}

Status IndexUpdater::UpdateTagIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                                    const kqir::Value &current, const SearchKey &search_key,
                                    const TagFieldMetadata *tag) const {
  CHECK(original.IsNull() || original.Is<kqir::StringArray>());
  CHECK(current.IsNull() || current.Is<kqir::StringArray>());
  auto original_tags = original.IsNull() ? std::vector<std::string>() : original.Get<kqir::StringArray>();
  auto current_tags = current.IsNull() ? std::vector<std::string>() : current.Get<kqir::StringArray>();

  auto to_tag_set = [](const std::vector<std::string> &tags, bool case_sensitive) -> std::set<std::string> {
    if (case_sensitive) {
      return {tags.begin(), tags.end()};
    } else {
      std::set<std::string> res;
      std::transform(tags.begin(), tags.end(), std::inserter(res, res.begin()), util::ToLower);
      return res;
    }
  };

  std::set<std::string> tags_to_delete = to_tag_set(original_tags, tag->case_sensitive);
  std::set<std::string> tags_to_add = to_tag_set(current_tags, tag->case_sensitive);

  for (auto it = tags_to_delete.begin(); it != tags_to_delete.end();) {
    if (auto jt = tags_to_add.find(*it); jt != tags_to_add.end()) {
      it = tags_to_delete.erase(it);
      tags_to_add.erase(jt);
    } else {
      ++it;
    }
  }

  if (tags_to_add.empty() && tags_to_delete.empty()) {
    // no change, skip index updating
    return Status::OK();
  }

  auto *storage = indexer->storage;
  auto batch = storage->GetWriteBatchBase();
  auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);

  for (const auto &tag : tags_to_delete) {
    auto index_key = search_key.ConstructTagFieldData(tag, key);

    auto s = batch->Delete(cf_handle, index_key);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
  }

  for (const auto &tag : tags_to_add) {
    auto index_key = search_key.ConstructTagFieldData(tag, key);

    auto s = batch->Put(cf_handle, index_key, Slice());
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
  }

  auto s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
}

Status IndexUpdater::UpdateNumericIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                                        const kqir::Value &current, const SearchKey &search_key,
                                        [[maybe_unused]] const NumericFieldMetadata *num) const {
  CHECK(original.IsNull() || original.Is<kqir::Numeric>());
  CHECK(current.IsNull() || current.Is<kqir::Numeric>());

  auto *storage = indexer->storage;
  auto batch = storage->GetWriteBatchBase();
  auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);

  if (!original.IsNull()) {
    auto index_key = search_key.ConstructNumericFieldData(original.Get<kqir::Numeric>(), key);

    auto s = batch->Delete(cf_handle, index_key);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
  }

  if (!current.IsNull()) {
    auto index_key = search_key.ConstructNumericFieldData(current.Get<kqir::Numeric>(), key);

    auto s = batch->Put(cf_handle, index_key, Slice());
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
  }
  auto s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
}

Status IndexUpdater::UpdateHnswVectorIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                                           const kqir::Value &current, const SearchKey &search_key,
                                           HnswVectorFieldMetadata *vector) {
  CHECK(original.IsNull() || original.Is<kqir::NumericArray>());
  CHECK(current.IsNull() || current.Is<kqir::NumericArray>());

  // TODO: we can remove the lock if we solve the race problem
  // inside the HNSW indexer, refer to #2481 and #2489
  std::unique_lock lock(update_mutex);

  auto storage = indexer->storage;
  auto hnsw = HnswIndex(search_key, vector, storage);

  if (!original.IsNull()) {
    auto batch = storage->GetWriteBatchBase();
    GET_OR_RET(hnsw.DeleteVectorEntry(ctx, key, batch));
    auto s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }

  if (!current.IsNull()) {
    auto batch = storage->GetWriteBatchBase();
    GET_OR_RET(hnsw.InsertVectorEntry(ctx, key, current.Get<kqir::NumericArray>(), batch));
    auto s = storage->Write(ctx, storage->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }

  return Status::OK();
}

Status IndexUpdater::UpdateIndex(engine::Context &ctx, const std::string &field, std::string_view key,
                                 const kqir::Value &original, const kqir::Value &current) {
  if (original == current) {
    // the value of this field is unchanged, no need to update
    return Status::OK();
  }

  auto iter = info->fields.find(field);
  if (iter == info->fields.end()) {
    return {Status::NotOK, "No such field to do index updating"};
  }

  auto *metadata = iter->second.metadata.get();
  SearchKey search_key(info->ns, info->name, field);
  if (auto tag = dynamic_cast<TagFieldMetadata *>(metadata)) {
    GET_OR_RET(UpdateTagIndex(ctx, key, original, current, search_key, tag));
  } else if (auto numeric [[maybe_unused]] = dynamic_cast<NumericFieldMetadata *>(metadata)) {
    GET_OR_RET(UpdateNumericIndex(ctx, key, original, current, search_key, numeric));
  } else if (auto vector = dynamic_cast<HnswVectorFieldMetadata *>(metadata)) {
    GET_OR_RET(UpdateHnswVectorIndex(ctx, key, original, current, search_key, vector));
  } else {
    return {Status::NotOK, "Unexpected field type"};
  }

  return Status::OK();
}

Status IndexUpdater::Update(engine::Context &ctx, const FieldValues &original, std::string_view key) {
  auto current = GET_OR_RET(Record(ctx, key));

  for (const auto &[field, i] : info->fields) {
    if (i.metadata->noindex) {
      continue;
    }

    kqir::Value original_val, current_val;

    if (auto it = original.find(field); it != original.end()) {
      original_val = it->second;
    }
    if (auto it = current.find(field); it != current.end()) {
      current_val = it->second;
    }

    GET_OR_RET(UpdateIndex(ctx, field, key, original_val, current_val));
  }

  return Status::OK();
}

Status IndexUpdater::Build(engine::Context &ctx) {
  auto storage = indexer->storage;
  util::UniqueIterator iter(ctx, ctx.DefaultScanOptions(), ColumnFamilyID::Metadata);

  for (const auto &prefix : info->prefixes) {
    auto ns_key = ComposeNamespaceKey(info->ns, prefix, storage->IsSlotIdEncoded());
    for (iter->Seek(ns_key); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(ns_key)) {
        break;
      }

      auto [_, key] = ExtractNamespaceKey(iter->key(), storage->IsSlotIdEncoded());

      auto s = Update(ctx, {}, key.ToStringView());
      if (s.Is<Status::TypeMismatched>()) continue;
      if (!s.OK()) return s;
    }

    if (auto s = iter->status(); !s.ok()) {
      return {Status::NotOK, s.ToString()};
    }
  }

  return Status::OK();
}

void GlobalIndexer::Add(std::unique_ptr<IndexUpdater> updater) {
  updater->indexer = this;
  for (const auto &prefix : updater->info->prefixes) {
    prefix_map.insert(ComposeNamespaceKey(updater->info->ns, prefix, false), updater.get());
  }
  updater_list.push_back(std::move(updater));
}

void GlobalIndexer::Remove(const kqir::IndexInfo *index) {
  for (auto iter = prefix_map.begin(); iter != prefix_map.end();) {
    if ((*iter)->info == index) {
      iter = prefix_map.erase(iter);
    } else {
      ++iter;
    }
  }

  updater_list.erase(
      std::remove_if(updater_list.begin(), updater_list.end(),
                     [index](const std::unique_ptr<IndexUpdater> &updater) { return updater->info == index; }),
      updater_list.end());
}

StatusOr<GlobalIndexer::RecordResult> GlobalIndexer::Record(engine::Context &ctx, std::string_view key,
                                                            const std::string &ns) {
  if (updater_list.empty()) {
    return Status::NoPrefixMatched;
  }

  auto iter = prefix_map.longest_prefix(ComposeNamespaceKey(ns, key, false));
  if (iter != prefix_map.end()) {
    auto updater = iter.value();
    return RecordResult{updater, std::string(key.begin(), key.end()), GET_OR_RET(updater->Record(ctx, key))};
  }

  return {Status::NoPrefixMatched};
}

Status GlobalIndexer::Update(engine::Context &ctx, const RecordResult &original) {
  return original.updater->Update(ctx, original.fields, original.key);
}

}  // namespace redis
