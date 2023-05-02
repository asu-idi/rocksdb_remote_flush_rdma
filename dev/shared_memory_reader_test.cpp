#include <sys/fcntl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/write_batch_base.h"
#include "util/logger.hpp"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/root/remove/rocksdb/"

using namespace std;
using namespace rocksdb;
struct ipc_data {
  int written_count{0};
  const static size_t MAXNUM = 1000;
  std::array<char, MAXNUM> payload;
};

auto main() -> signed {
  for (int i = 8; i <= 30; i++) {
    // shmctl(i);
    shmctl(i, IPC_RMID, nullptr);
  }
  return 0;
  key_t key1;
  key_t key2;
  if (-1 != open("/tmp/foo", O_CREAT, 0777)) {
    key1 = ftok("/tmp/foo4", 0);
  } else {
    perror("open");
    exit(1);
  }
  if (-1 != open("/tmp/foo2", O_CREAT, 0777)) {
    key2 = ftok("/tmp/foo5", 0);
  } else {
    perror("open");
    exit(1);
  }
  printf("KEY: %x %x\n", key1, key2);
  int id1 = shmget(key1, 0x1000, IPC_CREAT | SHM_R | SHM_W);
  int id2 = shmget(key2, 0x2000, IPC_CREAT | SHM_R | SHM_W);
  printf("MID: %x %x\n", id1, id2);
  void *addr1 = shmat(id1, 0, 0);
  if (addr1 == (void *)-1) perror("shmat1");
  void *addr2 = shmat(id2, 0, 0);
  if (addr2 == (void *)-1) perror("shmat2");
  printf("ADDR:%p %p\n", addr1, addr2);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  shmctl(id1, IPC_RMID, NULL);
  shmctl(id2, IPC_RMID, NULL);
  // test: alloc 10 blocks
  // for (int i = 1; i <= 10; i++) {
  //   std::string now = ("/tmp/sjald" + std::to_string(i));
  //   int shmid = shmget(ftok(now.c_str(), 0), 4096, 0644 | IPC_CREAT);
  //   LOG("shmget name=", now, " id=", shmid);
  // }

  // int shmid = shmget(ftok("/tmp/shard1", 0), 4096, 0644 | IPC_CREAT);

  // void* shm_ptr = shmat(shmid, nullptr, 0);
  // std::atomic<char*> ptr_[1];
  // ptr_[0] = static_cast<char*>(shm_ptr);
  // __builtin_prefetch(ptr_[0], 0, 1);

  // LOG("Reader vir_ptr =", shm_ptr);

  // while (true) {
  //   auto* data_ = static_cast<ipc_data*>(shm_ptr);
  //   if (data_->written_count == 0) {
  //     std::this_thread::sleep_for(std::chrono::seconds(2));
  //     continue;
  //   }
  //   data_->written_count--;
  //   LOG("reader detect data: ", data_->payload.data());
  //   break;
  // }
  // shmdt(shm_ptr);
  return 0;
}