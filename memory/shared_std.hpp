#pragma once
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

#include "memory/shared_mem_basic.h"

namespace shm_std {
template <typename T>
class STDSharedMemoryAllocator {
 public:
  using value_type = T;

  STDSharedMemoryAllocator() = default;

  template <typename U>
  STDSharedMemoryAllocator(const STDSharedMemoryAllocator<U> &other) noexcept {}

  auto allocate(std::size_t n) -> T * {
    LOG("allocate", n);
    return static_cast<T *>(static_cast<void *>(shm_alloc(n * sizeof(T))));
  }

  void deallocate(T *p, std::size_t n) noexcept {
    LOG("deallocate", n, ' ', std::hex, p);
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
template <typename T>
using shared_vector = std::vector<T, STDSharedMemoryAllocator<T>>;
template <typename T>
using shared_deque = std::deque<T, STDSharedMemoryAllocator<T>>;

// note: this isn't gonna work, the allocator is ignored
// using shared_string = std::basic_string<char, std::char_traits<char>,
// STDSharedMemoryAllocator<char>>;
using shared_char_vector = std::vector<char, STDSharedMemoryAllocator<char>>;

template <typename T>
using shared_list = std::list<T, STDSharedMemoryAllocator<T>>;

}  // namespace shm_std
