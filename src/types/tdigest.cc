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

#include "tdigest.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <queue>

#include "fmt/format.h"
#include "glog/logging.h"
#include "status.h"

namespace {

// reference:
// https://github.com/apache/arrow/blob/a08037f33f2fe00763032623e18ba049d19a024f/cpp/src/arrow/util/tdigest.cc a
// numerically stable lerp is unbelievably complex but we are *approximating* the quantile, so let's keep it simple
double Lerp(double a, double b, double t) { return a + t * (b - a); }

// scale function K0: linear function, as baseline
struct ScalerK0 {
  explicit ScalerK0(uint32_t delta) : delta_norm(delta / 2.0) {}

  double K(double q) const { return delta_norm * q; }
  double Q(double k) const { return k / delta_norm; }

  const double delta_norm;
};

// scale function K1
struct ScalerK1 {
  explicit ScalerK1(uint32_t delta) : delta_norm(delta / (2.0 * M_PI)) {}

  double K(double q) const { return delta_norm * std::asin(2 * q - 1); }
  double Q(double k) const { return (std::sin(k / delta_norm) + 1) / 2; }

  const double delta_norm;
};
}  // namespace

class TDigest {
 public:
  explicit TDigest(uint64_t delta);

  TDigest(const TDigest&) = delete;
  TDigest& operator=(const TDigest&) = delete;
  TDigest(TDigest&& rhs) = default;
  ~TDigest() = default;

  void Merge(const std::vector<TDigest>& others);
  void Add(std::vector<double> items);
  void Reset(const std::vector<Centroid>& centroids);
  CentroidsWithDelta DumpCentroids() const;

 private:
  class TDigestImpl;
  std::unique_ptr<TDigestImpl> impl_;
};

template <typename T = ScalerK1>
class TDigestMerger : private T {
 public:
  using Status = rocksdb::Status;
  explicit TDigestMerger(uint32_t delta) : T(delta) { Reset(0, nullptr); }

  void Reset(double total_weight, std::vector<Centroid>* tdigest) {
    total_weight_ = total_weight;
    tdigest_ = tdigest;
    if (tdigest_) {
      tdigest_->resize(0);
    }
    weight_so_far_ = 0;
    weight_limit_ = -1;  // trigger first centroid merge
  }

  // merge one centroid from a sorted centroid stream
  void Add(const Centroid& centroid) {
    auto& td = *tdigest_;
    const double weight = weight_so_far_ + centroid.weight;
    if (weight <= weight_limit_) {
      td.back().Merge(centroid);
    } else {
      const double quantile = weight_so_far_ / total_weight_;
      const double next_weight_limit = total_weight_ * this->Q(this->K(quantile) + 1);
      // weight limit should be strictly increasing, until the last centroid
      if (next_weight_limit <= weight_limit_) {
        weight_limit_ = total_weight_;
      } else {
        weight_limit_ = next_weight_limit;
      }
      td.push_back(centroid);  // should never exceed capacity and trigger reallocation
    }
    weight_so_far_ = weight;
  }

  // validate k-size of a tdigest
  Status Validate(const std::vector<Centroid>& tdigest, double total_weight) const {
    double q_prev = 0, k_prev = this->K(0);
    for (const auto& i : tdigest) {
      const double q = q_prev + i.weight / total_weight;
      const double k = this->K(q);
      if (i.weight != 1 && (k - k_prev) > 1.001) {
        return Status::Corruption(fmt::format("oversized centroid: {}", k - k_prev));
      }
      k_prev = k;
      q_prev = q;
    }
    return Status::OK();
  }

 private:
  double total_weight_;   // total weight of this tdigest
  double weight_so_far_;  // accumulated weight till current bin
  double weight_limit_;   // max accumulated weight to move to next bin
  std::vector<Centroid>* tdigest_;
};

class TDigest::TDigestImpl {
 public:
  using Status = rocksdb::Status;
  explicit TDigestImpl(uint32_t delta) : delta_(delta > 10 ? delta : 10), merger_(delta_) {
    tdigests_[0].reserve(delta_);
    tdigests_[1].reserve(delta_);
    Reset();
  }

  void Reset() {
    tdigests_[0].resize(0);
    tdigests_[1].resize(0);
    current_ = 0;
    total_weight_ = 0;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
    merger_.Reset(0, nullptr);
  }

  void Reset(const std::vector<Centroid>& centroids) {
    tdigests_[0] = centroids;
    tdigests_[1].resize(0);
    current_ = 0;
    total_weight_ = 0;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
    merger_.Reset(0, nullptr);
  }

  Status Validate() const {
    // check weight, centroid order
    double total_weight = 0, prev_mean = std::numeric_limits<double>::lowest();
    for (const auto& centroid : tdigests_[current_]) {
      if (std::isnan(centroid.mean) || std::isnan(centroid.weight)) {
        return Status::Corruption("NAN found in tdigest");
      }
      if (centroid.mean < prev_mean) {
        return Status::Corruption("centroid mean decreases");
      }
      if (centroid.weight < 1) {
        return Status::Corruption("invalid centroid weight");
      }
      prev_mean = centroid.mean;
      total_weight += centroid.weight;
    }
    if (total_weight != total_weight_) {
      return Status::Corruption("tdigest total weight mismatch");
    }
    // check if buffer expanded
    if (tdigests_[0].capacity() > delta_ || tdigests_[1].capacity() > delta_) {
      return Status::Corruption("oversized tdigest buffer");
    }
    // check k-size
    return merger_.Validate(tdigests_[current_], total_weight_);
  }

  std::vector<Centroid> Centroids() const { return tdigests_[current_]; }

  uint32_t Delta() const { return delta_; }

  // merge with other tdigests
  void Merge(const std::vector<const TDigestImpl*>& tdigest_impls) {
    // current and end iterator
    using CentroidIter = std::vector<Centroid>::const_iterator;
    using CentroidIterPair = std::pair<CentroidIter, CentroidIter>;
    // use a min-heap to find next minimal centroid from all tdigests
    auto centroid_gt = [](const CentroidIterPair& lhs, const CentroidIterPair& rhs) {
      return lhs.first->mean > rhs.first->mean;
    };
    using CentroidQueue = std::priority_queue<CentroidIterPair, std::vector<CentroidIterPair>, decltype(centroid_gt)>;

    // trivial dynamic memory allocated at runtime
    std::vector<CentroidIterPair> queue_buffer;
    queue_buffer.reserve(tdigest_impls.size() + 1);
    CentroidQueue queue(std::move(centroid_gt), std::move(queue_buffer));

    const auto& this_tdigest = tdigests_[current_];
    if (this_tdigest.size() > 0) {
      queue.emplace(this_tdigest.cbegin(), this_tdigest.cend());
    }
    for (const TDigestImpl* td : tdigest_impls) {
      const auto& other_tdigest = td->tdigests_[td->current_];
      if (other_tdigest.size() > 0) {
        queue.emplace(other_tdigest.cbegin(), other_tdigest.cend());
        total_weight_ += td->total_weight_;
        min_ = std::min(min_, td->min_);
        max_ = std::max(max_, td->max_);
      }
    }

    merger_.Reset(total_weight_, &tdigests_[1 - current_]);
    CentroidIter current_iter, end_iter;
    // do k-way merge till one buffer left
    while (queue.size() > 1) {
      std::tie(current_iter, end_iter) = queue.top();
      merger_.Add(*current_iter);
      queue.pop();
      if (++current_iter != end_iter) {
        queue.emplace(current_iter, end_iter);
      }
    }
    // merge last buffer
    if (!queue.empty()) {
      std::tie(current_iter, end_iter) = queue.top();
      while (current_iter != end_iter) {
        merger_.Add(*current_iter++);
      }
    }
    merger_.Reset(0, nullptr);

    current_ = 1 - current_;
  }

  // merge input data with current tdigest
  void MergeInput(std::vector<double>& input) {
    if (tdigests_[current_].empty() && !input.empty()) {
      min_ = input.front();
      max_ = input.front();
    }
    total_weight_ += static_cast<double>(input.size());

    std::sort(input.begin(), input.end());
    LOG(INFO) << "input size: " << input.size();
    if (input.empty()) {
      LOG(INFO) << "empty input";
      return;
    }
    LOG(INFO) << " front: " << input.front() << " back: " << input.back() << "min_: " << min_ << " max_: " << max_;
    min_ = std::min(min_, input.front());
    max_ = std::max(max_, input.back());

    // pick next minimal centroid from input and tdigest, feed to merger
    merger_.Reset(total_weight_, &tdigests_[1 - current_]);
    const auto& td = tdigests_[current_];
    uint32_t tdigest_index = 0, input_index = 0;
    while (tdigest_index < td.size() && input_index < input.size()) {
      if (td[tdigest_index].mean < input[input_index]) {
        merger_.Add(td[tdigest_index++]);
      } else {
        merger_.Add(Centroid{input[input_index++], 1});
      }
    }
    while (tdigest_index < td.size()) {
      merger_.Add(td[tdigest_index++]);
    }
    while (input_index < input.size()) {
      merger_.Add(Centroid{input[input_index++], 1});
    }
    merger_.Reset(0, nullptr);

    input.resize(0);
    current_ = 1 - current_;
  }

  double Quantile(double q) const {
    const auto& td = tdigests_[current_];

    if (q < 0 || q > 1 || td.size() == 0) {
      return NAN;
    }

    const double index = q * total_weight_;
    if (index <= 1) {
      return min_;
    } else if (index >= total_weight_ - 1) {
      return max_;
    }

    // find centroid contains the index
    uint32_t ci = 0;
    double weight_sum = 0;
    for (; ci < td.size(); ++ci) {
      weight_sum += td[ci].weight;
      if (index <= weight_sum) {
        break;
      }
    }
    DCHECK_LT(ci, td.size());

    // deviation of index from the centroid center
    double diff = index + td[ci].weight / 2 - weight_sum;

    // index happen to be in a unit weight centroid
    if (td[ci].weight == 1 && std::abs(diff) < 0.5) {
      return td[ci].mean;
    }

    // find adjacent centroids for interpolation
    uint32_t ci_left = ci, ci_right = ci;
    if (diff > 0) {
      if (ci_right == td.size() - 1) {
        // index larger than center of last bin
        DCHECK_EQ(weight_sum, total_weight_);
        const Centroid* c = &td[ci_right];
        DCHECK_GE(c->weight, 2);
        return Lerp(c->mean, max_, diff / (c->weight / 2));
      }
      ++ci_right;
    } else {
      if (ci_left == 0) {
        // index smaller than center of first bin
        const Centroid* c = &td[0];
        DCHECK_GE(c->weight, 2);
        return Lerp(min_, c->mean, index / (c->weight / 2));
      }
      --ci_left;
      diff += td[ci_left].weight / 2 + td[ci_right].weight / 2;
    }

    // interpolate from adjacent centroids
    diff /= (td[ci_left].weight / 2 + td[ci_right].weight / 2);
    return Lerp(td[ci_left].mean, td[ci_right].mean, diff);
  }

  double Mean() const {
    double sum = 0;
    for (const auto& centroid : tdigests_[current_]) {
      sum += centroid.mean * centroid.weight;
    }
    return total_weight_ == 0 ? NAN : sum / total_weight_;
  }

  double TotalWeight() const { return total_weight_; }

 private:
  // must be declared before merger_, see constructor initialization list
  const uint32_t delta_;

  TDigestMerger<> merger_;
  double total_weight_;
  double min_, max_;

  // ping-pong buffer holds two tdigests, size = 2 * delta * sizeof(Centroid)
  std::vector<Centroid> tdigests_[2];
  // index of active tdigest buffer, 0 or 1
  int current_;
};

TDigest::TDigest(uint64_t delta) : impl_(std::make_unique<TDigestImpl>(delta)) { Reset({}); }

void TDigest::Merge(const std::vector<TDigest>& others) {
  if (others.empty()) {
    return;
  }

  std::vector<const TDigestImpl*> impls;
  impls.reserve(others.size());

  std::transform(others.cbegin(), others.cend(), std::back_inserter(impls),
                 [](const TDigest& i) { return i.impl_.get(); });

  impl_->Merge(impls);
}

void TDigest::Reset(const std::vector<Centroid>& centroids) { impl_->Reset(centroids); }

CentroidsWithDelta TDigest::DumpCentroids() const {
  auto centroids = impl_->Centroids();
  return {std::move(centroids), impl_->Delta()};
}

void TDigest::Add(std::vector<double> items) { impl_->MergeInput(items); }

StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<CentroidsWithDelta>& centroids_list) {
  if (centroids_list.empty()) {
    return Status{Status::InvalidArgument, "centroids_list is empty"};
  }
  if (centroids_list.size() == 1) {
    return centroids_list.front();
  }

  TDigest digest{centroids_list.front().delta};
  digest.Reset(centroids_list.front().centroids);

  std::vector<TDigest> others;
  others.reserve(centroids_list.size() - 1);

  for (size_t i = 1; i < centroids_list.size(); ++i) {
    TDigest d{centroids_list[i].delta};
    digest.Reset(centroids_list[i].centroids);
    others.emplace_back(std::move(d));
  }

  digest.Merge(others);

  return digest.DumpCentroids();
}
StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<double>& buffer, const CentroidsWithDelta& centroid_list) {
  TDigest digest{centroid_list.delta};
  digest.Reset(centroid_list.centroids);
  digest.Add(buffer);
  return digest.DumpCentroids();
}