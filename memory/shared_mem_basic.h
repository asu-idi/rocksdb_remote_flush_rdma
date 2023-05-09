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
static char* shm_alloc(size_t bytes) {
  // char* ptr = static_cast<char*>(malloc(bytes));
  LOG("shared mem arena alloc: ", bytes);
  std::string rnd_mmap_name = "/tmp/";
  std::mt19937 rnd_gen{std::random_device{}()};
  for (int i = 1; i <= 20; i++) {
    rnd_mmap_name.push_back(rnd_gen() % 26 + 'a');
  }

  assert(-1 != open(rnd_mmap_name.c_str(), O_CREAT, 0777));
  key_t fkey = ftok(rnd_mmap_name.c_str(), 0);

  int shmid = shmget(fkey, bytes, IPC_CREAT | SHM_R | SHM_W);
  assert(shmid != -1);
  auto* ptr = static_cast<char*>(shmat(shmid, nullptr, 0));
  LOG("shared mem arena alloc ptr: ", std::hex, (long long)ptr, std::dec,
      " mid = ", shmid, " filename= ", rnd_mmap_name);
  return ptr;
}
static void shm_delete(char* ptr) {
  LOG("call deleter: ", std::hex, (long long)ptr, std::dec);
  shmdt(ptr);  // detach

  // free(ptr);
}