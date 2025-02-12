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

#include "types/cms.h"

#include <gtest/gtest.h>

#include <memory>

#include "test_base.h"
#include "types/redis_cms.h"

class RedisCMSketchTest : public TestBase {
 protected:
  using IncrByPair = redis::CMS::IncrByPair;
  explicit RedisCMSketchTest() : TestBase() { cms_ = std::make_unique<redis::CMS>(storage_.get(), "cms_ns"); }
  ~RedisCMSketchTest() override = default;

  void SetUp() override {
    TestBase::SetUp();
    [[maybe_unused]] auto s = cms_->Del(*ctx_, "cms");
    for (int x = 1; x <= 3; x++) {
      s = cms_->Del(*ctx_, "cms" + std::to_string(x));
    }
  }

  void TearDown() override {
    TestBase::TearDown();
    [[maybe_unused]] auto s = cms_->Del(*ctx_, "cms");
    for (int x = 1; x <= 3; x++) {
      s = cms_->Del(*ctx_, "cms" + std::to_string(x));
    }
  }

  std::unique_ptr<redis::CMS> cms_;
};

TEST_F(RedisCMSketchTest, CMSInitByDim) {
  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms", 100, 5).ok());
  CMSketch::CMSInfo info;
  ASSERT_TRUE(cms_->Info(*ctx_, "cms", &info).ok());
  ASSERT_EQ(info.width, 100);
  ASSERT_EQ(info.depth, 5);
  ASSERT_EQ(info.count, 0);
}

TEST_F(RedisCMSketchTest, CMSIncrBy) {
  std::vector<IncrByPair> elements = {{"apple", 2}, {"banana", 3}, {"cherry", 1}};
  // TODO(mwish): check the responses.
  std::vector<uint32_t> responses;
  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms", 100, 5).ok());
  ASSERT_TRUE(cms_->IncrBy(*ctx_, "cms", elements, &responses).ok());

  std::vector<uint32_t> counts;
  ASSERT_TRUE(cms_->Query(*ctx_, "cms", {"apple", "banana", "cherry"}, counts).ok());

  ASSERT_EQ(counts[0], 2);
  ASSERT_EQ(counts[1], 3);
  ASSERT_EQ(counts[2], 1);

  CMSketch::CMSInfo info;
  ASSERT_TRUE(cms_->Info(*ctx_, "cms", &info).ok());
  ASSERT_EQ(info.count, 6);
}

TEST_F(RedisCMSketchTest, CMSQuery) {
  std::vector<IncrByPair> elements = {{"orange", 5}, {"grape", 3}, {"melon", 2}};
  std::vector<uint32_t> responses;
  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms", 100, 5).ok());
  ASSERT_TRUE(cms_->IncrBy(*ctx_, "cms", elements, &responses).ok());

  std::vector<uint32_t> counts;
  ASSERT_TRUE(cms_->Query(*ctx_, "cms", {"orange", "grape", "melon", "nonexistent"}, counts).ok());

  ASSERT_EQ(counts[0], 5);
  ASSERT_EQ(counts[1], 3);
  ASSERT_EQ(counts[2], 2);
  ASSERT_EQ(counts[3], 0);
}

TEST_F(RedisCMSketchTest, CMSInfo) {
  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms", 200, 10).ok());

  CMSketch::CMSInfo info;
  ASSERT_TRUE(cms_->Info(*ctx_, "cms", &info).ok());

  ASSERT_EQ(info.width, 200);
  ASSERT_EQ(info.depth, 10);
  ASSERT_EQ(info.count, 0);
}

TEST_F(RedisCMSketchTest, CMSInitByProb) {
  ASSERT_TRUE(cms_->InitByProb(*ctx_, "cms", 0.001, 0.1).ok());

  CMSketch::CMSInfo info;
  ASSERT_TRUE(cms_->Info(*ctx_, "cms", &info).ok());

  ASSERT_EQ(info.width, 2000);
  ASSERT_EQ(info.depth, 4);
  ASSERT_EQ(info.count, 0);
}

TEST_F(RedisCMSketchTest, CMSMultipleKeys) {
  std::vector<IncrByPair> elements1 = {{"apple", 2}, {"banana", 3}};
  std::vector<IncrByPair> elements2 = {{"cherry", 1}, {"date", 4}};
  std::vector<uint32_t> responses;

  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms1", 100, 5).ok());
  ASSERT_TRUE(cms_->InitByDim(*ctx_, "cms2", 100, 5).ok());

  ASSERT_TRUE(cms_->IncrBy(*ctx_, "cms1", elements1, &responses).ok());
  ASSERT_TRUE(cms_->IncrBy(*ctx_, "cms2", elements2, &responses).ok());

  std::vector<uint32_t> counts1, counts2;
  ASSERT_TRUE(cms_->Query(*ctx_, "cms1", {"apple", "banana"}, counts1).ok());
  ASSERT_TRUE(cms_->Query(*ctx_, "cms2", {"cherry", "date"}, counts2).ok());

  ASSERT_EQ(counts1[0], 2);
  ASSERT_EQ(counts1[1], 3);
  ASSERT_EQ(counts2[0], 1);
  ASSERT_EQ(counts2[1], 4);
}
