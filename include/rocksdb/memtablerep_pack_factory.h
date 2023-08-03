#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "memory/remote_flush_service.h"
#include "memtable/readonly_inlineskiplist.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "util/logger.hpp"
#include "util/socket_api.hpp"
#ifdef __linux__
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif  //__linux__

#ifdef ROCKSDB_ALL_TESTS_ENABLED
#include "test_util/testutil.h"
#endif  // ROCKSDB_ALL_TESTS_ENABLED

namespace ROCKSDB_NAMESPACE {
class MemTableRepPackFactory {
 public:
  static void* UnPackLocal(char*& buf);
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
  int64_t info = *msg >> 8;
  if (type == 1 /*SkipListRep*/) {
    void* local_skiplistrep = ReadOnlySkipListRep::UnPackLocal(node);
    return local_skiplistrep;
  } else if (type == 2 /*Test SpecialMemTableRep*/) {
    LOG("MemTableRepPackFactory::UnPackLocal: SpecialMemTableRep: ", type, ' ',
        info);
    void* sub_memtable_ = UnPackLocal(node);
    auto memtable_ = reinterpret_cast<MemTableRep*>(sub_memtable_);
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
}

inline void* MemTableRepPackFactory::UnPackLocal(char*& buf) {
  int64_t msg = 0;
  UNPACK_FROM_BUF(buf, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = msg >> 8;
  if (type == 1 /*SkipListRep*/) {
    LOG("MemTableRepPackFactory::UnPackLocal: SkipListRep");
    void* local_skiplistrep = ReadOnlySkipListRep::UnPackLocal(buf);
    return local_skiplistrep;
  } else {
    LOG("MemTableRepPackFactory::UnPackLocal error", type, ' ', info);
    assert(false);
    return nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE