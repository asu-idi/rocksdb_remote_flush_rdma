#pragma once
#include <deque>
#include <list>
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

  auto allocate(std::size_t n) -> T * {
    return static_cast<T *>(static_cast<void *>(shm_alloc(n * sizeof(T))));
  }

  void deallocate(T *p, std::size_t n) noexcept {
    return shm_delete(reinterpret_cast<char *>(p));
  }

  template <class T1>
  auto operator==(const STDSharedMemoryAllocator<T1> &) -> bool {
    return true;
  }
  template <class T1>
  auto operator!=(const STDSharedMemoryAllocator<T1> &) -> bool {
    return false;
  }
};
namespace shm_std {
template <typename T>
using shared_vector = std::vector<T, STDSharedMemoryAllocator<T>>;
template <typename T>
using shared_deque = std::deque<T, STDSharedMemoryAllocator<T>>;
template <typename T>
using shared_string =
    std::basic_string<T, std::char_traits<T>, STDSharedMemoryAllocator<T>>;

template <typename T>
using shared_list = std::list<T, STDSharedMemoryAllocator<T>>;

}  // namespace shm_std