// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/comparator_factory.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {

// Wrapper of user comparator, with auto increment to
// perf_context.user_key_comparison_count.
class UserComparatorWrapper {
 public:
  void PackLocal(int sockfd) const {
    user_comparator_->PackLocal(sockfd);
    send(sockfd, reinterpret_cast<const void*>(this), sizeof(*this), 0);
    int64_t ret_val = 0;
    read_data(sockfd, &ret_val, sizeof(int64_t));
  }
  static void* UnPackLocal(int sockfd) {
    void* ucmp = ComparatorFactory::UnPackLocal(sockfd);
    void* mem = malloc(sizeof(UserComparatorWrapper));
    read_data(sockfd, mem, sizeof(UserComparatorWrapper));
    auto* ret = reinterpret_cast<UserComparatorWrapper*>(mem);
    ret->user_comparator_ = reinterpret_cast<Comparator*>(ucmp);
    send(sockfd, &ret, sizeof(void*), 0);
    return ret;
  }

 public:
  // `UserComparatorWrapper`s constructed with the default constructor are not
  // usable and will segfault on any attempt to use them for comparisons.
  UserComparatorWrapper() : user_comparator_(nullptr) {}

  explicit UserComparatorWrapper(const Comparator* const user_cmp)
      : user_comparator_(const_cast<Comparator*>(user_cmp)) {}

  ~UserComparatorWrapper() = default;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const Slice& a, const Slice& b) const {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Compare(a, b);
  }

  bool Equal(const Slice& a, const Slice& b) const {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Equal(a, b);
  }

  int CompareTimestamp(const Slice& ts1, const Slice& ts2) const {
    return user_comparator_->CompareTimestamp(ts1, ts2);
  }

  int CompareWithoutTimestamp(const Slice& a, const Slice& b) const {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->CompareWithoutTimestamp(a, b);
  }

  int CompareWithoutTimestamp(const Slice& a, bool a_has_ts, const Slice& b,
                              bool b_has_ts) const {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->CompareWithoutTimestamp(a, a_has_ts, b, b_has_ts);
  }

  bool EqualWithoutTimestamp(const Slice& a, const Slice& b) const {
    return user_comparator_->EqualWithoutTimestamp(a, b);
  }

 private:
  const Comparator* user_comparator_;
};

}  // namespace ROCKSDB_NAMESPACE
