#pragma once

#include "memory/remote_flush_service.h"
#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#endif
#include <cstdint>
#include <memory>
#include <string>

#include "db/dbformat.h"
#include "memory/remote_flush_service.h"
#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice_transform.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {
class SliceTransformFactory {
 public:
  static void* UnPackLocal(char*& buf);
  static void* UnPackLocal(TCPNode* node);

  SliceTransformFactory(const SliceTransformFactory&) = delete;
  void operator=(const SliceTransformFactory&) = delete;

 private:
  SliceTransformFactory() = default;
  ~SliceTransformFactory() = default;
};

inline void* SliceTransformFactory::UnPackLocal(TCPNode* node) {
  int64_t* msg = nullptr;
  size_t size = sizeof(int64_t);
  node->receive(reinterpret_cast<void**>(&msg), &size);
  int64_t type = *msg & 0xff;
  int64_t info = (*msg >> 8);
  if (type == 0 /*InternalKeySliceTransform*/) {
    void* ptr = SliceTransformFactory::UnPackLocal(node);
    return reinterpret_cast<void*>(InternalKeySliceTransform::UnPackLocal(ptr));
  } else if (type == 1 /*FixedPrefixListRep*/) {
    const SliceTransform* local_ptr = NewFixedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 2 /*CappedPrefixTransform*/) {
    const SliceTransform* local_ptr = NewCappedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 3 /*NoopTransform*/) {
    const SliceTransform* local_ptr = NewNoopTransform();
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 0xff) {
    return nullptr;
  } else {
    LOG("SliceTransformFactory::UnPackLocal: error: ", type, ' ', info);
    assert(false);
    return nullptr;
  }
}

inline void* SliceTransformFactory::UnPackLocal(char*& buf) {
  int64_t msg = 0;
  UNPACK_FROM_BUF(buf, &msg, sizeof(msg));
  int64_t type = msg & 0xff;
  int64_t info = (msg >> 8);
  if (type == 0 /*InternalKeySliceTransform*/) {
    void* ptr = SliceTransformFactory::UnPackLocal(buf);
    return reinterpret_cast<void*>(
        InternalKeySliceTransform::UnPackLocal(ptr, buf));
  } else if (type == 1 /*FixedPrefixListRep*/) {
    const SliceTransform* local_ptr = NewFixedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 2 /*CappedPrefixTransform*/) {
    const SliceTransform* local_ptr = NewCappedPrefixTransform(info);
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 3 /*NoopTransform*/) {
    const SliceTransform* local_ptr = NewNoopTransform();
    return reinterpret_cast<void*>(const_cast<SliceTransform*>(local_ptr));
  } else if (type == 0xff) {
    return nullptr;
  } else {
    LOG("SliceTransformFactory::UnPackLocal: error: ", type, ' ', info);
    assert(false);
    return nullptr;
  }
}

}  // namespace ROCKSDB_NAMESPACE

// ROCKSDB_ALL_TESTS_ENABLED