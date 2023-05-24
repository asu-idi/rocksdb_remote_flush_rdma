//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/concurrent_shared_arena.h"

#include <algorithm>

#include "logging/logging.h"
#include "memory/shared_mem_basic.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/logger.hpp"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

ConSharedArena* ConSharedArena::CreateSharedConSharedArena(
    size_t block_size, AllocTracker* tracker, size_t huge_page_size) {
  void* mem = shm_alloc(sizeof(ConSharedArena));
  auto* arena = new (mem) ConSharedArena(block_size, tracker, huge_page_size);
  return arena;
}

size_t ConSharedArena::OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(ConSharedArena::kMinBlockSize, block_size);
  block_size = std::min(ConSharedArena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

ConSharedArena::ConSharedArena(size_t block_size, AllocTracker* tracker,
                               size_t huge_page_size)
    : inline_block_(shm_alloc(kInlineSize)),
      kBlockSize(OptimizeBlockSize(block_size)),
      tracker_(tracker) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  TEST_SYNC_POINT_CALLBACK("ConSharedArena::ConSharedArena:0",
                           const_cast<size_t*>(&kBlockSize));
  alloc_bytes_remaining_ = kInlineSize;
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
}

ConSharedArena::~ConSharedArena() {
  if (tracker_ != nullptr) {
    assert(tracker_->is_freed());
    tracker_->FreeMem();
  }
  shm_delete(inline_block_);
}

char* ConSharedArena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    LOG("fallback use AllocateNewBlock");
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
  if (!block_head) {
    size = kBlockSize;
    LOG("fallback use AllocateNewBlock");
    block_head = AllocateNewBlock(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    LOG("aligned, now aligned_alloc_ptr_: ", std::hex,
        (long long)aligned_alloc_ptr_,
        " unaligned_alloc_ptr_: ", (long long)unaligned_alloc_ptr_, std::dec);
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    LOG("not align aligned_alloc_ptr_: ", std::hex, aligned_alloc_ptr_,
        " unaligned_alloc_ptr_: ", unaligned_alloc_ptr_, std::dec);
    return unaligned_alloc_ptr_;
  }
}

char* ConSharedArena::AllocateFromHugePage(size_t bytes) {
  char* addr = AllocateNewBlock(bytes);
  if (addr) {
    blocks_memory_ += bytes;
    if (tracker_ != nullptr) {
      tracker_->Allocate(bytes);
    }
  }
  return addr;
}

char* ConSharedArena::AllocateAligned(size_t bytes, size_t huge_page_size,
                                      Logger* logger) {
  if (MemMapping::kHugePageSupported && hugetlb_size_ > 0 &&
      huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TLB table.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);
    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     errnoStr(errno).c_str());
      // fail back to malloc
    } else {
      return addr;
    }
  }

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    LOG("allocate fallback to normal block");
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* ConSharedArena::AllocateNewBlock(size_t block_bytes) {
  // NOTE: std::make_unique zero-initializes the block so is not appropriate
  // here
  assert(block_bytes >= 4096 && block_bytes % 4096 == 0);
  LOG("alloc new block with size=", block_bytes);
  //   char* block = new char[block_bytes];
  char* block = shm_alloc(block_bytes);

  blocks_.emplace_back(
      std::unique_ptr<char, void (*)(char* p)>(block, [](char* p) {
        LOG("shared_mem free: ", p);
        shm_delete(p);
      }));

  size_t allocated_size;
  allocated_size = block_bytes;
  blocks_memory_ += allocated_size;
  if (tracker_ != nullptr) {
    tracker_->Allocate(allocated_size);
  }
  return block;
}

}  // namespace ROCKSDB_NAMESPACE
