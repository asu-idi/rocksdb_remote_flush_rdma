#pragma once

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
char* shm_alloc(size_t bytes);
void shm_delete(char* ptr);