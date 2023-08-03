//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memory/concurrent_arena.h"

#include <sys/socket.h>

#include <thread>

#include "memory/remote_flush_service.h"
#include "port/port.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

thread_local size_t ConcurrentArena::tls_cpuid = 0;

namespace {
// If the shard block size is too large, in the worst case, every core
// allocates a block without populate it. If the shared block size is
// 1MB, 64 cores will quickly allocate 64MB, and may quickly trigger a
// flush. Cap the size instead.
const size_t kMaxShardBlockSize = size_t{128 * 1024};
}  // namespace

ConcurrentArena::ConcurrentArena(size_t block_size, AllocTracker* tracker,
                                 size_t huge_page_size)
    : shard_block_size_(std::min(kMaxShardBlockSize, block_size / 8)),
      shards_(),
      arena_(block_size, tracker, huge_page_size) {
  Fixup();
}

ConcurrentArena::Shard* ConcurrentArena::Repick() {
  auto shard_and_index = shards_.AccessElementAndIndex();
  // even if we are cpu 0, use a non-zero tls_cpuid so we can tell we
  // have repicked
  tls_cpuid = shard_and_index.second | shards_.Size();
  return shard_and_index.first;
}

void ConcurrentArena::PackLocal(TransferService* node) const {
  std::string name = "ConcurrentArena";
  name.resize(15);
  node->send(name.data(), 15);
}
void* ConcurrentArena::UnPackLocal(TransferService* node) {
  void* arena = reinterpret_cast<void*>(new ConcurrentArena());
  return arena;
}
void ConcurrentArena::PackLocal(char*& buf) const {
  std::string name = "ConcurrentArena";
  name.resize(15);
  PACK_TO_BUF(name.data(), buf, name.size());
}
void* ConcurrentArena::UnPackLocal(char*& buf) {
  void* arena = reinterpret_cast<void*>(new ConcurrentArena());
  return arena;
}

}  // namespace ROCKSDB_NAMESPACE
