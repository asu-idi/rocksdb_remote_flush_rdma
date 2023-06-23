#pragma once

#include <cstddef>
#include <map>
#include <set>
#ifdef __linux
#include <sys/fcntl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#endif  //__linux

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "util/logger.hpp"
char *shm_alloc(size_t bytes);
void shm_delete(char *ptr);

class noncopyable {
 public:
  noncopyable() = default;
  virtual ~noncopyable() = default;

  noncopyable(const noncopyable &) = delete;
  noncopyable(noncopyable &&) = delete;
  auto operator=(const noncopyable &) noexcept -> noncopyable & = delete;
  auto operator=(noncopyable &&) noexcept -> noncopyable & = delete;
};
template <class T>
class singleton : public noncopyable {
 protected:
  singleton() = default;
  ~singleton() override = default;

 public:
  static auto Instance() -> T & {
    static T instance;
    return instance;
  }
};

class SharedContainer {
 public:
  std::map<void *, size_t> mp_area_;
  auto insert(void *ptr, size_t size) -> bool {
    if (!legal(ptr, size)) return false;
    mp_area_.insert({ptr, size});
    return true;
  }
  auto remove(void *ptr) -> bool {
    if (mp_area_.find(ptr) != mp_area_.end()) {
      mp_area_.erase(ptr);
      return true;
    }
    return false;
  }
  auto find(void *ptr, size_t size) const -> bool {
    void *front = ptr, *back = reinterpret_cast<char *>(ptr) + size;
    for (auto &pr : mp_area_) {
      void *front2 = pr.first,
           *back2 = reinterpret_cast<char *>(pr.first) + pr.second;
      //  TODO: check different continuous blocks
      if (front2 <= front && back <= back2) return true;
    }
    return false;
  }
  [[nodiscard]] auto size() const -> size_t { return mp_area_.size(); }

  auto debug() {
    for (auto &pr : mp_area_) {
      LOG(std::hex, pr.first, " ",
          reinterpret_cast<void *>(reinterpret_cast<char *>(pr.first) +
                                   pr.second))
    }
  }

 private:
  bool legal(void *ptr, size_t size) {
    if (ptr == nullptr) return false;
    if (size == 0) return false;

    for (auto &pr : mp_area_) {
      void *front = pr.first,
           *back = reinterpret_cast<char *>(pr.first) + pr.second;
      void *insert_front = ptr,
           *insert_back = reinterpret_cast<char *>(ptr) + size;
      if (insert_front >= front && insert_front < back) return false;
      if (insert_back > front && insert_back <= back) return false;
    }
    return true;
  }
};