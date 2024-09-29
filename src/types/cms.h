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

#include <memory>
#include <vector>

#include "server/redis_reply.h"

class CMSketch {
 public:
  explicit CMSketch(uint32_t width, uint32_t depth, uint64_t counter, std::vector<uint32_t> array)
      : width_(width),
        depth_(depth),
        counter_(counter),
        array_(array.empty() ? std::vector<uint32_t>(width * depth, 0) : std::move(array)) {}

  struct CMSInfo {
    uint32_t width;
    uint32_t depth;
    uint64_t count;
  };

  struct CMSketchDimensions {
    uint32_t width;
    uint32_t depth;
  };

  static CMSketchDimensions CMSDimFromProb(double error, double delta);

  size_t IncrBy(std::string_view item, uint32_t value);

  size_t Query(std::string_view item) const;

  struct MergeParams {
    CMSketch* dest;
    size_t num_keys;
    std::vector<const CMSketch*> cms_array;
    std::vector<long long> weights;
  };

  static Status Merge(const MergeParams& params);

  size_t GetLocationForHash(uint64_t hash, size_t i) const { return (hash % width_) + (i * width_); }

  uint64_t& GetCounter() { return counter_; }
  std::vector<uint32_t>& GetArray() { return array_; }

  const uint64_t& GetCounter() const { return counter_; }
  const std::vector<uint32_t>& GetArray() const { return array_; }

  uint32_t GetWidth() const { return width_; }
  uint32_t GetDepth() const { return depth_; }

 private:
  uint32_t width_;
  uint32_t depth_;
  uint64_t counter_;
  std::vector<uint32_t> array_;

  static int checkOverflow(CMSketch* dest, size_t quantity, const std::vector<const CMSketch*>& src,
                           const std::vector<long long>& weights);
};