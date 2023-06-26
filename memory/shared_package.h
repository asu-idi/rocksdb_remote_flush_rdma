#pragma once

#include "memory/shared_mem_basic.h"
#include "memory/shared_std.hpp"

namespace shm_package {

// shm_std::shared_vector<std::pair<void *, size_t>> package_;

char *Pack(const std::string &raw);

void Unpack(void *u, std::string &t, size_t length);

}  // namespace shm_package