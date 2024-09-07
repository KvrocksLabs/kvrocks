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

#include "cms.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

void CMSketch::CMSDimFromProb(double error, double delta, uint32_t& width, uint32_t& depth) {
  width = std::ceil(2 / error);
  depth = std::ceil(std::log10(delta) / std::log10(0.5));
}

size_t CMSketch::IncrBy(const char* item, size_t item_len, size_t value) {
  size_t min_count = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < depth_; ++i) {
    uint32_t hash = HllMurMurHash64A(item, static_cast<int>(item_len), i);
    size_t loc = (hash % width_) + (i * width_);
    array_[loc] += value;
    if (array_[loc] < value) {
      array_[loc] = UINT32_MAX;
    }
    min_count = std::min(min_count, static_cast<size_t>(array_[loc]));
  }
  counter_ += value;
  return min_count;
}

size_t CMSketch::Query(const char* item, size_t item_len) const {
  size_t min_count = std::numeric_limits<size_t>::max();

  for (size_t i = 0; i < depth_; ++i) {
    uint32_t hash = HllMurMurHash64A(item, static_cast<int>(item_len), i);
    min_count = std::min(min_count, static_cast<size_t>(array_[(hash % width_) + (i * width_)]));
  }
  return min_count;
}

int CMSketch::Merge(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                    const std::vector<long long>& weights) {
  if (checkOverflow(dest, quantity, src, weights) != 0) {
    return -1;
  }

  for (size_t i = 0; i < dest->GetDepth(); ++i) {
    for (size_t j = 0; j < dest->GetWidth(); ++j) {
      int64_t item_count = 0;
      for (size_t k = 0; k < quantity; ++k) {
        item_count += static_cast<int64_t>(src[k]->array_[(i * dest->GetWidth()) + j]) * weights[k];
      }
      dest->GetArray()[(i * dest->GetWidth()) + j] = item_count;
    }
  }

  for (size_t i = 0; i < quantity; ++i) {
    dest->GetCounter() += src[i]->GetCounter() * weights[i];
  }

  return 0;
}

int CMSMergeParams(const CMSketch::MergeParams& params) {
  return CMSketch::Merge(params.dest, params.num_keys, params.cms_array, params.weights);
}

int CMSketch::checkOverflow(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                            const std::vector<long long>& weights) {
  int64_t item_count = 0;
  int64_t cms_count = 0;
  size_t width = dest->GetWidth();
  size_t depth = dest->GetDepth();

  for (size_t i = 0; i < depth; ++i) {
    for (size_t j = 0; j < width; ++j) {
      item_count = 0;
      for (size_t k = 0; k < quantity; ++k) {
        int64_t mul = 0;

        if (__builtin_mul_overflow(src[k]->GetArray()[(i * width) + j], weights[k], &mul) ||
            (__builtin_add_overflow(item_count, mul, &item_count))) {
          return -1;
        }
      }

      if (item_count < 0 || item_count > UINT32_MAX) {
        return -1;
      }
    }
  }

  for (size_t i = 0; i < quantity; ++i) {
    int64_t mul = 0;

    if (__builtin_mul_overflow(src[i]->GetCounter(), weights[i], &mul) ||
        (__builtin_add_overflow(cms_count, mul, &cms_count))) {
      return -1;
    }
  }

  if (cms_count < 0) {
    return -1;
  }

  return 0;
}