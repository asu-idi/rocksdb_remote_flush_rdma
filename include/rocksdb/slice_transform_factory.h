#pragma once

#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#endif
#include <cstdint>
#include <memory>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice_transform.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {
class SliceTransformFactory {
 public:
  static void* UnPackLocal(int sockfd);

 private:
  SliceTransformFactory() = default;
  SliceTransformFactory(const SliceTransformFactory&) = delete;
  void operator=(const SliceTransformFactory&) = delete;
  ~SliceTransformFactory() = default;
};

inline void* SliceTransformFactory::UnPackLocal(int sockfd) {
  int64_t msg = 0;
  read(sockfd, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = (msg >> 8);
  if (type == 0 /*InternalKeySliceTransform*/) {
    send(sockfd, &msg, sizeof(msg), 0);
    void* ptr = SliceTransformFactory::UnPackLocal(sockfd);
    return reinterpret_cast<void*>(
        InternalKeySliceTransform::UnPackLocal(ptr, sockfd));
  } else if (type == 1 /*FixedPrefixListRep*/) {
    send(sockfd, &msg, sizeof(msg), 0);
    const SliceTransform* local_ptr = NewFixedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 2 /*CappedPrefixTransform*/) {
    send(sockfd, &msg, sizeof(msg), 0);
    const SliceTransform* local_ptr = NewCappedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 3 /*NoopTransform*/) {
    send(sockfd, &msg, sizeof(msg), 0);
    const SliceTransform* local_ptr = NewNoopTransform();
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else {
    LOG("SliceTransformFactory::UnPackLocal: error: ", type, ' ', info);
    assert(false);
    return nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE

// ROCKSDB_ALL_TESTS_ENABLED