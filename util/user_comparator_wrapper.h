// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "memory/shared_mem_basic.h"
#include "memory/shared_std.hpp"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {

// Wrapper of user comparator, with auto increment to
// perf_context.user_key_comparison_count.
class UserComparatorWrapper {
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

  // shared
  bool is_packaged_ = false;
  void Pack();
  void UnPack();
  bool is_shared();
  const char* shm_name_ = nullptr;

 private:
  // const
  Comparator* user_comparator_;
};

inline bool UserComparatorWrapper::is_shared() {
  return singleton<SharedContainer>::Instance().find(
      reinterpret_cast<void*>(this), sizeof(UserComparatorWrapper));
}
inline void UserComparatorWrapper::Pack() {
  assert(is_shared());
  if (is_packaged_) {
    LOG("UserComparatorWrapper is already packaged");
    return;
  }
  // TODO(copy): construct a new comparator

  LOG("UserComparatorWrapper::Pack() data: ", user_comparator_->Name());
  shm_name_ = user_comparator_->Pack();

  is_packaged_ = true;
}

inline void UserComparatorWrapper::UnPack() {
  assert(is_shared());
  if (!is_packaged_) {
    LOG("UserComparatorWrapper is already unpackaged");
    return;
  }
  // TODO(copy): construct a new comparator
  if (strcmp(shm_name_, "leveldb.BytewiseComparator") == 0) {
    auto* cmp = BytewiseComparator();
    user_comparator_ = static_cast<Comparator*>(const_cast<Comparator*>(cmp));
  } else if (strcmp(shm_name_, "rocksdb.ReverseBytewiseComparator") == 0) {
    auto* cmp = ReverseBytewiseComparator();
    user_comparator_ = static_cast<Comparator*>(const_cast<Comparator*>(cmp));
  } else {
    LOG("UserComparatorWrapper::UnPack() is unimplemented: ", shm_name_);
    assert(false);
  }
  user_comparator_->UnPack(shm_name_);
  is_packaged_ = false;
}

}  // namespace ROCKSDB_NAMESPACE
