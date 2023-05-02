#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/shared_mem_basic.h"
#include "memory/shared_memory_allocator.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/mmap.h"
#include "rocksdb/env.h"
#include "rocksdb/thread_status.h"
#include "util/core_local.h"
#include "util/logger.hpp"
#include "util/mutexlock.h"
#include "util/thread_local.h"
#ifdef __linux
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#endif  //__linux
#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
#else
#define ROCKSDB_FIELD_UNUSED
#endif  // __clang__
namespace ROCKSDB_NAMESPACE {
class Logger;

// TODO: multi thread
class SharedArena : public Allocator {
 public:
  SharedArena(const SharedArena&) = delete;
  void operator=(const SharedArena&) = delete;
  explicit SharedArena(size_t block_size = Arena::kMinBlockSize,
                       AllocTracker* tracker = nullptr);
  ~SharedArena();

  char* Allocate(size_t bytes) override;
  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override;

  size_t ApproximateMemoryUsage() const { return current_block_usage_; }

  size_t MemoryAllocatedBytes() const { return current_block_usage_; }

  size_t AllocatedAndUnused() const { return 0; }

  size_t BlockSize() const override { return 0; }

  bool IsInInlineBlock() const { return false; }

 private:
  static constexpr size_t SHMMIN = 4096;
  size_t block_count_ = 0;
  // static const size_t SHMMAX = 18446744073692774399;
  size_t current_block_usage_ = 0;
  std::map<int, std::pair<std::unique_ptr<char, void (*)(char*)>, size_t> >
      mp_blocks_;
  AllocTracker* tracker_ = nullptr;
};

inline char* SharedArena::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes < SHMMIN) {
    LOG("alloc bytes less than SHMMIN, alloc ", SHMMIN);
    bytes = SHMMIN;
  }
  LOG("shared mem arena alloc: ", bytes);
  auto* ptr = shm_alloc(bytes);
  LOG("shared mem arena alloc ptr: ", std::hex, (long long)ptr, std::dec);
  std::unique_ptr<char, void (*)(char* p)> uni_ptr(ptr, [](char* p) {
    LOG("shared_mem free: ", p);
    shm_delete(p);
  });
  mp_blocks_.insert(std::make_pair(++block_count_,
                                   std::make_pair(std::move(uni_ptr), bytes)));
  current_block_usage_ += bytes;
  return ptr;
}

inline char* SharedArena::AllocateAligned(size_t bytes, size_t huge_page_size,
                                          Logger* logger) {
  assert(logger == nullptr);
  // TODO: remove this
  assert(huge_page_size == 0);
  size_t rounded_up = ((bytes - 1) | (sizeof(void*) - 1)) + 1;
  assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void*) &&
         (rounded_up % sizeof(void*)) == 0);
  return Allocate(rounded_up);
}

}  // namespace ROCKSDB_NAMESPACE