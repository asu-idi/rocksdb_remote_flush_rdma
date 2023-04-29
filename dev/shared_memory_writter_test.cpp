#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
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

#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;
struct ipc_data {
  int written_count{0};
  const static size_t MAXNUM = 1000;
  std::array<char, MAXNUM> payload;
};

auto main() -> signed {
  int shmid = shmget(ftok("/data/rocksdb/dev/shared1", 0), sizeof(ipc_data),
                     0666 | IPC_CREAT);
  LOG("shmget id=", shmid);
  void* shm_ptr = shmat(shmid, nullptr, 0);
  LOG("writter vir_ptr =", shm_ptr);
  ipc_data data;
  LOG("before write: size=", data.payload.size());
  for (int i = 1; i <= 26; i++) {
    char a1 = rand() % 26 + 'a';
    data.payload[i - 1] = a1;
  }
  data.written_count++;
  memcpy(shm_ptr, (void*)&data, sizeof(ipc_data));
  LOG("write data: ", data.payload.data());

  return 0;
}