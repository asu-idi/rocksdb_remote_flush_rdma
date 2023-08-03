#pragma once

#include <stdint.h>
#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>

#include "memtable/readonly_inlineskiplist.h"
#include "memory/remote_flush_service.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "util/logger.hpp"
#ifdef __linux__
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif  //__linux__

namespace ROCKSDB_NAMESPACE {
class MemTableRepPackFactory {
 public:
  static void* UnPackLocal(int sockfd);
  static void* UnPackLocal(char*& buf);
  MemTableRepPackFactory(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(const MemTableRepPackFactory&) = delete;
  MemTableRepPackFactory& operator=(MemTableRepPackFactory&&) = delete;

 private:
  MemTableRepPackFactory() = default;
  ~MemTableRepPackFactory() = default;
};

inline void* MemTableRepPackFactory::UnPackLocal(int sockfd) {
  int64_t msg = 0;
  read(sockfd, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = msg >> 8;
  if (type == 1 /*SkipListRep*/) {
    send(sockfd, &msg, sizeof(msg), 0);
    LOG("MemTableRepPackFactory::UnPackLocal: SkipListRep");
    void* local_skiplistrep = ReadOnlySkipListRep::UnPackLocal(sockfd);
    return local_skiplistrep;
  } else {
    LOG("MemTableRepPackFactory::UnPackLocal error", type, ' ', info);
    assert(false);
    return nullptr;
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