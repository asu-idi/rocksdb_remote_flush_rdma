#include "memory/shared_memory_allocator.h"

#include <sys/ipc.h>
#include <sys/shm.h>

#include <random>
#include <utility>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
Status SharedMemoryAllocator::PrepareOptions(const ConfigOptions& options) {
  if (!IsSupported()) {
    return Status::NotSupported("not support linux shared_memory");
  } else {
    return MemoryAllocator::PrepareOptions(options);
  }
  return Status::OK();
}

#ifdef __linux
void* SharedMemoryAllocator::Allocate(size_t size) {
  void* ptr = nullptr;
  std::string rnd;
  std::mt19937 rnd_gen{std::random_device{}()};
  for (int i = 1; i <= 50; i++) {
    rnd.push_back(rnd_gen() % 26 + 'a');
  }
  int shmid = shmget(ftok(("/tmp/" + rnd).c_str(), 0), size, 0666 | IPC_CREAT);
  ptr = shmat(shmid, nullptr, 0);
  mp_ptr_size.insert(std::make_pair(ptr, size));
  return ptr;
}
void SharedMemoryAllocator::Deallocate(void* p) { shmdt(p); }
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
size_t SharedMemoryAllocator::UsableSize(void* p,
                                         size_t /*allocation_size*/) const {
  assert(mp_ptr_size.find(p) != mp_ptr_size.end());
  return mp_ptr_size.at(p);
}
#endif  // ROCK

#endif  //__linux

}  // namespace ROCKSDB_NAMESPACE