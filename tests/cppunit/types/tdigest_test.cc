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

#include "types/tdigest.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <random>
#include <range/v3/algorithm/shuffle.hpp>
#include <range/v3/range.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <string>
#include <vector>

#include "storage/redis_metadata.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_tdigest.h"

namespace {
constexpr std::random_device::result_type kSeed = 14863;  // fixed seed for reproducibility

std::vector<double> QuantileOf(const std::vector<double> &samples, const std::vector<double> &qs) {
  std::vector<double> result;
  result.reserve(qs.size());
  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());
  for (auto q : qs) {
    auto index = q * static_cast<double>(sorted_samples.size());
    if (index <= 1) {
      result.push_back(sorted_samples.front());
    } else if (index >= static_cast<double>(sorted_samples.size() - 1)) {
      result.push_back(sorted_samples.back());
    } else {
      auto left = sorted_samples[static_cast<int>(index)];
      auto right = sorted_samples[static_cast<int>(index) + 1];
      auto diff = index - static_cast<int>(index);
      result.push_back(left + (right - left) * diff);
    }
  }
  return result;
}

std::vector<std::pair<double, double>> QuantileIntervalOf(const std::vector<double> &samples,
                                                          const std::vector<double> &qs) {
  std::vector<std::pair<double, double>> result;
  result.reserve(qs.size());
  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());
  for (auto q : qs) {
    auto index = q * static_cast<double>(sorted_samples.size());
    if (index <= 1) {
      result.emplace_back(sorted_samples.front(), sorted_samples.front());
    } else if (index >= static_cast<double>(sorted_samples.size() - 1)) {
      result.emplace_back(sorted_samples.back(), sorted_samples.back());
    } else {
      auto left = sorted_samples[static_cast<int>(index)];
      auto right = sorted_samples[static_cast<int>(index) + 1];
      result.emplace_back(left, right);
    }
  }
  return result;
}

std::vector<double> GenerateSamples(int count, double from, double to) {
  std::vector<double> samples;
  samples.reserve(count);
  for (int i = 0; i < count; i++) {
    samples.push_back(from + static_cast<double>(i) * (to - from) / static_cast<double>(count));
  }
  return samples;
}

std::vector<double> GenerateQuantiles(int count, bool with_head = false, bool with_tail = false) {
  std::vector<double> qs;
  qs.reserve(count);
  for (int i = 1; i <= count; i++) {
    qs.push_back(static_cast<double>(i) / static_cast<double>(count));
  }
  if (with_head) {
    qs.insert(qs.begin(), 0);
  }
  if (with_tail) {
    qs.push_back(1);
  }
  return qs;
}

}  // namespace

class RedisTDigestTest : public TestBase {
 protected:
  RedisTDigestTest() : name_("tdigest_test") {
    tdigest_ = std::make_unique<redis::TDigest>(storage_.get(), "tdigest_ns");
  }

  std::string name_;
  std::unique_ptr<redis::TDigest> tdigest_;
};

TEST_F(RedisTDigestTest, CentroidTest) {
  Centroid c1{
      2.,
      3.,
  };
  Centroid c2{
      3.,
      4.,
  };

  c1.Merge(c2);

  EXPECT_NEAR(c1.weight, 7., 0.01);
  EXPECT_NEAR(c1.mean, 2.57, 0.01);
}

TEST_F(RedisTDigestTest, Create) {
  std::string test_digest_name = "test_digest_create" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_TRUE(exists);
  ASSERT_TRUE(status.IsInvalidArgument());

  TDigestMetadata metadata;
  auto get_status = tdigest_->GetMetaData(*ctx_, test_digest_name, &metadata);
  ASSERT_TRUE(get_status.ok()) << get_status.ToString();
  ASSERT_EQ(metadata.compression, 100) << metadata.compression;
}

TEST_F(RedisTDigestTest, Quantile) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());
  std::vector<double> samples = ranges::views::iota(1, 101) | ranges::views::transform([](int i) { return i; }) |
                                ranges::to<std::vector<double>>();

  status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<double> qs = {0.5, 0.9, 0.99};
  redis::TDigestQuantitleResult result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(result.quantiles.size(), qs.size());
  EXPECT_NEAR(result.quantiles[0], 50.5, 0.01);
  EXPECT_NEAR(result.quantiles[1], 90.5, 0.01);
  EXPECT_NEAR(result.quantiles[2], 100, 0.01);
}

TEST_F(RedisTDigestTest, PlentyQuantile_10000_144) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());
  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  int sample_count = 10000;
  int quantile_count = 144;
  double from = -100;
  double to = 100;
  auto error_double = (to - from) / sample_count;
  auto samples = GenerateSamples(sample_count, -100, 100);
  status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto qs = GenerateQuantiles(quantile_count);
  auto result = QuantileOf(samples, qs);

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();

  for (int i = 0; i < quantile_count; i++) {
    EXPECT_NEAR(tdigest_result.quantiles[i], result[i], error_double) << "quantile is: " << qs[i];
  }
}

TEST_F(RedisTDigestTest, Add_2_times) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());

  bool exists = false;
  auto status = tdigest_->Create(*ctx_, test_digest_name, {100}, &exists);
  ASSERT_FALSE(exists);
  ASSERT_TRUE(status.ok());

  int sample_count = 17;
  int quantile_count = 7;
  auto samples = GenerateSamples(sample_count, -100, 100);
  auto qs = GenerateQuantiles(quantile_count);
  auto expect_result = QuantileIntervalOf(samples, qs);
  std::shuffle(samples.begin(), samples.end(), std::mt19937(kSeed));

  int group_count = 4;
  auto samples_sub_group =
      samples | ranges::views::chunk(sample_count / group_count) | ranges::to<std::vector<std::vector<double>>>();

  for (const auto &s : samples_sub_group) {
    status = tdigest_->Add(*ctx_, test_digest_name, s);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();

  for (int i = 0; i < quantile_count; i++) {
    auto &[expect_down, expect_upper] = expect_result[i];
    auto got = tdigest_result.quantiles[i];
    EXPECT_GE(got, expect_down) << fmt::format("quantile is {}, should in interval [{}, {}]", qs[i], expect_down,
                                               expect_upper);
    EXPECT_LE(got, expect_upper) << fmt::format("quantile is {}, should in interval [{}, {}]", qs[i], expect_down,
                                                expect_upper);
  }
}
