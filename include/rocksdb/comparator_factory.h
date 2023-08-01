#pragma once

#include <cassert>
#include <string>

#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "memory/remote_flush_service.h"
#include "rocksdb/comparator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {

class ComparatorFactory {
 public:
  static void* UnPackLocal(int sockfd);
  static void* UnPackLocal(char*& buf);

  ComparatorFactory(const ComparatorFactory&) = delete;
  void operator=(const ComparatorFactory&) = delete;

 private:
  ComparatorFactory() = default;
  ~ComparatorFactory() = default;
};

inline void* ComparatorFactory::UnPackLocal(int sockfd) {
  int64_t msg = 0;
  read(sockfd, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = (msg >> 8);
  if (type == 0 /*BytewiseComparator*/) {
    LOG("ComparatorFactory::UnPackLocal: BytewiseComparator");
    send(sockfd, &msg, sizeof(msg), 0);
    const Comparator* local_ptr = BytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else if (type == 1 /*ReverseBytewiseComparatorImpl*/) {
    LOG("ComparatorFactory::UnPackLocal: ReverseBytewiseComparatorImpl");
    send(sockfd, &msg, sizeof(msg), 0);
    const Comparator* local_ptr = ReverseBytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else {
    LOG("ComparatorFactory::UnPackLocal: error: ", type, ' ', info);
    assert(false);
    return nullptr;
  }

  return nullptr;
}
inline void* ComparatorFactory::UnPackLocal(char*& buf) {
  int64_t msg = 0;
  UNPACK_FROM_BUF(buf, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = (msg >> 8);
  if (type == 0 /*BytewiseComparator*/) {
    LOG("ComparatorFactory::UnPackLocal: BytewiseComparator");
    const Comparator* local_ptr = BytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else if (type == 1 /*ReverseBytewiseComparatorImpl*/) {
    LOG("ComparatorFactory::UnPackLocal: ReverseBytewiseComparatorImpl");
    const Comparator* local_ptr = ReverseBytewiseComparator();
    return reinterpret_cast<void*>(const_cast<Comparator*>(local_ptr));
  } else {
    LOG("ComparatorFactory::UnPackLocal: error: ", type, ' ', info);
    assert(false);
    return nullptr;
  }

  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
