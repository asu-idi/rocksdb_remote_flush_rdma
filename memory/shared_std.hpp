#pragma once
#include <deque>
#include <memory>
#include <sstream>
#include <vector>

#include "memory/shared_mem_basic.h"

template <typename T>
class STDSharedMemoryAllocator {
 public:
  using value_type = T;

  STDSharedMemoryAllocator() = default;

  template <typename U>
  STDSharedMemoryAllocator(const STDSharedMemoryAllocator<U> &other) noexcept {}

  T *allocate(std::size_t n) {
    return static_cast<T *>(static_cast<void *>(shm_alloc(n * sizeof(T))));
  }

  void deallocate(T *p, std::size_t n) noexcept {
    return shm_delete(reinterpret_cast<char *>(p));
  }
};
namespace std {
template <typename T>
using shared_vector = vector<T, STDSharedMemoryAllocator<T>>;
template <typename T>
using shared_deque = deque<T, STDSharedMemoryAllocator<T>>;
template <typename T>
using shared_string =
    basic_string<T, char_traits<T>, STDSharedMemoryAllocator<T>>;

}  // namespace std