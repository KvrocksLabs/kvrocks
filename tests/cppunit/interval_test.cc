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

#include "search/interval.h"

#include <gtest/gtest.h>

#include <random>

#include "search/ir.h"

using namespace kqir;

TEST(IntervalSet, Simple) {
  ASSERT_TRUE(IntervalSet().IsEmpty());
  ASSERT_TRUE(!IntervalSet().IsFull());
  ASSERT_TRUE(IntervalSet(IntervalSet::full).IsFull());
  ASSERT_TRUE(!IntervalSet(IntervalSet::full).IsEmpty());
  ASSERT_TRUE((~IntervalSet()).IsFull());
  ASSERT_TRUE((~IntervalSet(IntervalSet::full)).IsEmpty());

  ASSERT_EQ(IntervalSet(Interval(1, 2)) | IntervalSet(Interval(2, 4)), IntervalSet(Interval(1, 4)));
  ASSERT_EQ((IntervalSet(Interval(1, 2)) | IntervalSet(Interval(2, 4))).intervals, (IntervalSet::DataType{{1, 4}}));
  ASSERT_EQ((IntervalSet(Interval(1, 2)) | IntervalSet(Interval(3, 4))).intervals,
            (IntervalSet::DataType{{1, 2}, {3, 4}}));
  ASSERT_EQ((IntervalSet(Interval(1, 4)) | IntervalSet(Interval(2, 3))).intervals, (IntervalSet::DataType{{1, 4}}));
  ASSERT_EQ((IntervalSet(Interval(2, 3)) | IntervalSet(Interval(1, 4))).intervals, (IntervalSet::DataType{{1, 4}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 1) | IntervalSet(NumericCompareExpr::LT, 4)).intervals,
            (IntervalSet::DataType{{IntervalSet::minf, IntervalSet::inf}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 1) | IntervalSet(NumericCompareExpr::NE, 4)).intervals,
            (IntervalSet::DataType{{IntervalSet::minf, IntervalSet::inf}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 4) | IntervalSet(NumericCompareExpr::LT, 1)).intervals,
            (IntervalSet::DataType{{IntervalSet::minf, 1}, {4, IntervalSet::inf}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 4) | IntervalSet(NumericCompareExpr::NE, 1)).intervals,
            (IntervalSet::DataType{{IntervalSet::minf, 1}, {IntervalSet::NextNum(1), IntervalSet::inf}}));

  ASSERT_TRUE((IntervalSet(Interval(1, 2)) & IntervalSet(Interval(3, 4))).IsEmpty());
  ASSERT_EQ((IntervalSet(Interval(1, 2)) & IntervalSet(Interval(2, 4))).intervals, (IntervalSet::DataType{{2, 2}}));
  ASSERT_EQ((IntervalSet(Interval(1, 3)) & IntervalSet(Interval(2, 4))).intervals, (IntervalSet::DataType{{2, 3}}));
  ASSERT_EQ((IntervalSet(Interval(3, 8)) & (IntervalSet(Interval(1, 4)) | IntervalSet(Interval(5, 7)))).intervals,
            (IntervalSet::DataType{{3, 4}, {5, 7}}));
  ASSERT_EQ((IntervalSet(Interval(3, 8)) & (IntervalSet(Interval(1, 4)) | IntervalSet(Interval(9, 11)))).intervals,
            (IntervalSet::DataType{{3, 4}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 1) & IntervalSet(NumericCompareExpr::LT, 4)).intervals,
            (IntervalSet::DataType{{1, 4}}));
  ASSERT_EQ((IntervalSet(NumericCompareExpr::GET, 1) & IntervalSet(NumericCompareExpr::NE, 4)).intervals,
            (IntervalSet::DataType{{1, 4}, {IntervalSet::NextNum(4), IntervalSet::inf}}));

  ASSERT_EQ(IntervalSet(IntervalSet::full) & IntervalSet(IntervalSet::full), IntervalSet(IntervalSet::full));
  ASSERT_EQ(IntervalSet(IntervalSet::full) | IntervalSet(IntervalSet::full), IntervalSet(IntervalSet::full));

  ASSERT_EQ((IntervalSet({1, 5}) | IntervalSet({7, 10})) & IntervalSet({2, 8}),
            IntervalSet({2, 5}) | IntervalSet({7, 8}));
  ASSERT_EQ(~IntervalSet({2, 8}), IntervalSet({IntervalSet::minf, 2}) | IntervalSet({8, IntervalSet::inf}));

  std::uniform_real_distribution<double> dist;
  std::uniform_int_distribution<int> dist_int(0, 50);
  std::random_device rd{};
  // Using random seed 0
  std::mt19937 rand_gen(rd()); // NOSONAR
  for (auto i = 0; i < 2000; ++i) {
    // generate random double
    auto gen = [&dist, &rand_gen] { return dist(rand_gen); };
    auto geni = [&dist_int, &rand_gen, &gen] {
      int r = dist_int(rand_gen);
      if (r == 0) {
        return IntervalSet(NumericCompareExpr::GET, gen());
      } else if (r == 1) {
        return IntervalSet(NumericCompareExpr::LT, gen());
      } else if (r == 2) {
        return IntervalSet(NumericCompareExpr::NE, gen());
      } else {
        return IntervalSet({gen(), gen()});
      }
    };

    auto l = geni(), r = geni();
    for (int j = 0; j < i % 10; ++j) {
      l = l | geni();
    }
    for (int j = 0; j < i % 7; ++j) {
      r = r | geni();
    }
    ASSERT_EQ(~l | ~r, ~(l & r));
    ASSERT_EQ(~l & ~r, ~(l | r));
  }
}
