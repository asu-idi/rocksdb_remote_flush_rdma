
#pragma once

#include <map>
#include <unordered_map>

#include "rocksdb/memory_allocator.h"
#include "utilities/memory_allocators.h"

#ifdef __linux
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#endif  //__linux

namespace ROCKSDB_NAMESPACE {

class SharedMemoryAllocator : public BaseMemoryAllocator {
 public:
  static const char* kClassName() { return "SharedMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }

  static bool IsSupported() {
#ifdef __linux
    int shmid = shmget(ftok("/tmp/rndnum1", 0), 8, 0666 | IPC_CREAT);
    void* shm_ptr = shmat(shmid, nullptr, 0);
    shmdt(shm_ptr);
    return true;
#else
    *msg = "Not compiled with SharedMemory";
    return false;
#endif
  }
  Status PrepareOptions(const ConfigOptions& options) override;

#ifdef __linux
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  size_t UsableSize(void* p, size_t /*allocation_size*/) const override;
#endif
#endif  //
 private:
  std::map<void*, size_t> mp_ptr_size;
};

}  // namespace ROCKSDB_NAMESPACE
