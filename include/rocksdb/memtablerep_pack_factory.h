#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "memtable/readonly_inlineskiplist.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/status.h"

#ifdef ROCKSDB_ALL_TESTS_ENABLED
#include "test_util/testutil.h"
#endif  // ROCKSDB_ALL_TESTS_ENABLED

namespace ROCKSDB_NAMESPACE {
class MemTableRepPackFactory {
 public:
  static void* UnPackLocal(TransferService* node);
  MemTableRepPackFactory(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(MemTableRepPackFactory&&) = delete;

 private:
  MemTableRepPackFactory() = default;
  ~MemTableRepPackFactory() = default;
};

inline void* MemTableRepPackFactory::UnPackLocal(TransferService* node) {
  int64_t* msg = nullptr;
  size_t size = sizeof(int64_t);
  node->receive(reinterpret_cast<void**>(&msg), &size);
  int64_t type = *msg & 0xff;
  [[maybe_unused]] int64_t info = *msg >> 8;
  if (type == 1 /*SkipListRep*/) {
    void* local_skiplistrep = ReadOnlySkipListRep::UnPackLocal(node);
    return local_skiplistrep;
  } else if (type == 2 /*Test SpecialMemTableRep*/) {
    LOG("MemTableRepPackFactory::UnPackLocal: SpecialMemTableRep: ", type, ' ',
        info);
    void* sub_memtable_ = UnPackLocal(node);
    [[maybe_unused]] auto memtable_ =
        reinterpret_cast<MemTableRep*>(sub_memtable_);
#ifdef ROCKSDB_ALL_TESTS_ENABLED
    int num_entries_flush = info;
    MemTableRepFactory* factory =
        test::NewSpecialSkipListFactory(num_entries_flush);
    MemTableRep* ret_memtable_ =
        factory->CreateExistMemTableWrapper(new ConcurrentArena(), memtable_);
    delete factory;
    return reinterpret_cast<void*>(ret_memtable_);
#else
    assert(false);
#endif  // ROCKSDB_ALL_TESTS_ENABLED
  } else {
    LOG("MemTableRepPackFactory::UnPackLocal error", type, ' ', info);
    assert(false);
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE