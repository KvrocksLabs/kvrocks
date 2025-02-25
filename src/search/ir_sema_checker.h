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

#include <map>
#include <memory>

#include "fmt/core.h"
#include "index_info.h"
#include "ir.h"
#include "search_encoding.h"
#include "storage/redis_metadata.h"

namespace kqir {

struct SemaChecker {
  const IndexMap &index_map;
  std::string ns;

  const IndexInfo *current_index = nullptr;

  explicit SemaChecker(const IndexMap &index_map) : index_map(index_map) {}

  Status Check(Node *node) {
    if (auto v = dynamic_cast<SearchExpr *>(node)) {
      auto index_name = v->index->name;
      if (auto iter = index_map.Find(index_name, ns); iter != index_map.end()) {
        current_index = iter->second.get();
        v->index->info = current_index;

        GET_OR_RET(Check(v->select.get()));
        GET_OR_RET(Check(v->query_expr.get()));
        if (v->limit) GET_OR_RET(Check(v->limit.get()));
        if (v->sort_by) GET_OR_RET(Check(v->sort_by.get()));
        if (v->sort_by && v->sort_by->IsVectorField()) {
          if (!v->limit) {
            return {Status::NotOK, "expect a LIMIT clause for vector field to construct a KNN search"};
          }
          // TODO: allow hybrid query
          if (auto b = dynamic_cast<BoolLiteral *>(v->query_expr.get()); b == nullptr) {
            return {Status::NotOK, "KNN search cannot be combined with other query expressions"};
          }
        }
      } else {
        return {Status::NotOK, fmt::format("index `{}` not found", index_name)};
      }
    } else if (auto v [[maybe_unused]] = dynamic_cast<LimitClause *>(node)) {
      return Status::OK();
    } else if (auto v = dynamic_cast<SortByClause *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else if (!iter->second.IsSortable()) {
        return {Status::NotOK, fmt::format("field `{}` is not sortable", v->field->name)};
      } else if (auto is_vector = iter->second.MetadataAs<redis::HnswVectorFieldMetadata>() != nullptr;
                 is_vector != v->IsVectorField()) {
        std::string not_str = is_vector ? "" : "not ";
        return {Status::NotOK,
                fmt::format("field `{}` is {}a vector field according to metadata and does {}expect a vector parameter",
                            v->field->name, not_str, not_str)};
      } else {
        v->field->info = &iter->second;
        if (v->IsVectorField()) {
          auto meta = v->field->info->MetadataAs<redis::HnswVectorFieldMetadata>();
          if (!v->field->info->HasIndex()) {
            return {Status::NotOK,
                    fmt::format("field `{}` is marked as NOINDEX and cannot be used for KNN search", v->field->name)};
          }
          if (v->vector->values.size() != meta->dim) {
            return {Status::NotOK,
                    fmt::format("vector should be of size `{}` for field `{}`", meta->dim, v->field->name)};
          }
        }
      }
    } else if (auto v = dynamic_cast<AndExpr *>(node)) {
      for (const auto &n : v->inners) {
        GET_OR_RET(Check(n.get()));
      }
    } else if (auto v = dynamic_cast<OrExpr *>(node)) {
      for (const auto &n : v->inners) {
        GET_OR_RET(Check(n.get()));
      }
    } else if (auto v = dynamic_cast<NotExpr *>(node)) {
      GET_OR_RET(Check(v->inner.get()));
    } else if (auto v = dynamic_cast<TagContainExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name)};
      } else if (auto meta = iter->second.MetadataAs<redis::TagFieldMetadata>(); !meta) {
        return {Status::NotOK, fmt::format("field `{}` is not a tag field", v->field->name)};
      } else {
        v->field->info = &iter->second;

        if (v->tag->val.empty()) {
          return {Status::NotOK, "tag cannot be an empty string"};
        }

        if (v->tag->val.find(meta->separator) != std::string::npos) {
          return {Status::NotOK, fmt::format("tag cannot contain the separator `{}`", meta->separator)};
        }
      }
    } else if (auto v = dynamic_cast<TextContainExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name)};
      } else if (auto meta = iter->second.MetadataAs<redis::TextFieldMetadata>(); !meta) {
        return {Status::NotOK, fmt::format("field `{}` is not a text field", v->field->name)};
      } else {
        v->field->info = &iter->second;

        if (v->word->val.empty()) {
          return {Status::NotOK, "word cannot be an empty string"};
        }
      }
    } else if (auto v = dynamic_cast<NumericCompareExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else if (!iter->second.MetadataAs<redis::NumericFieldMetadata>()) {
        return {Status::NotOK, fmt::format("field `{}` is not a numeric field", v->field->name)};
      } else {
        v->field->info = &iter->second;
      }
    } else if (auto v = dynamic_cast<VectorKnnExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else if (!iter->second.MetadataAs<redis::HnswVectorFieldMetadata>()) {
        return {Status::NotOK, fmt::format("field `{}` is not a vector field", v->field->name)};
      } else {
        v->field->info = &iter->second;

        if (!v->field->info->HasIndex()) {
          return {Status::NotOK,
                  fmt::format("field `{}` is marked as NOINDEX and cannot be used for KNN search", v->field->name)};
        }
        auto meta = v->field->info->MetadataAs<redis::HnswVectorFieldMetadata>();
        if (v->vector->values.size() != meta->dim) {
          return {Status::NotOK,
                  fmt::format("vector should be of size `{}` for field `{}`", meta->dim, v->field->name)};
        }
      }
    } else if (auto v = dynamic_cast<VectorRangeExpr *>(node)) {
      if (auto iter = current_index->fields.find(v->field->name); iter == current_index->fields.end()) {
        return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", v->field->name, current_index->name)};
      } else if (!iter->second.MetadataAs<redis::HnswVectorFieldMetadata>()) {
        return {Status::NotOK, fmt::format("field `{}` is not a vector field", v->field->name)};
      } else {
        v->field->info = &iter->second;

        auto meta = v->field->info->MetadataAs<redis::HnswVectorFieldMetadata>();
        if (meta->distance_metric == redis::DistanceMetric::L2 && v->range->val < 0) {
          return {Status::NotOK, "range cannot be a negative number for l2 distance metric"};
        }

        if (meta->distance_metric == redis::DistanceMetric::COSINE && (v->range->val < 0 || v->range->val > 2)) {
          return {Status::NotOK, "range has to be between 0 and 2 for cosine distance metric"};
        }

        if (v->vector->values.size() != meta->dim) {
          return {Status::NotOK,
                  fmt::format("vector should be of size `{}` for field `{}`", meta->dim, v->field->name)};
        }
      }
    } else if (auto v = dynamic_cast<SelectClause *>(node)) {
      for (const auto &n : v->fields) {
        if (auto iter = current_index->fields.find(n->name); iter == current_index->fields.end()) {
          return {Status::NotOK, fmt::format("field `{}` not found in index `{}`", n->name, current_index->name)};
        } else {
          n->info = &iter->second;
        }
      }
    } else if (auto v [[maybe_unused]] = dynamic_cast<BoolLiteral *>(node)) {
      return Status::OK();
    } else {
      return {Status::NotOK, fmt::format("unexpected IR node type: {}", node->Name())};
    }

    return Status::OK();
  }
};

}  // namespace kqir
