#pragma once

#include "memory/shared_mem_basic.h"
#include "memory/shared_std.hpp"

namespace shm_package {

// shm_std::shared_vector<std::pair<void *, size_t>> package_;

char *Pack(const std::string &raw) {
  char *ret = shm_alloc(raw.length());
  strncpy(ret, raw.data(), raw.length());
  return ret;
}

void Unpack(void *u, std::string &t, size_t length) {
  // assert(t.empty());
  t.resize(length);
  strncpy(t.data(), reinterpret_cast<char *>(u), length);
}

}  // namespace shm_package