#include "db/memtable.h"

#include <ifaddrs.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/options.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/root/code/rocksdb_remote_flush/"
using namespace std;
using namespace rocksdb;

std::string find_local_ip() {
  struct ifaddrs* ifaddr;
  int result1 = getifaddrs(&ifaddr);
  std::string ret;
  if (result1 != 0) {
    std::cerr << "getifaddrs failed: " << strerror(errno) << std::endl;
    assert(false);
  }

  for (struct ifaddrs* ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL || ifa->ifa_addr->sa_family != AF_INET) {
      continue;
    }
    struct sockaddr_in* addr =
        reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
    char ip[INET_ADDRSTRLEN];
    const char* result = inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    if (result == NULL) {
      std::cerr << "inet_ntop failed: " << strerror(errno) << std::endl;
      assert(false);
    }

    std::cout << "IP address for " << ifa->ifa_name << ": " << ip << std::endl;
    if (ifa->ifa_name[0] ==
        'e') {  // TODO(rdma): change this on your machine. eth0/ens33
      ret = ip;
    }
  }
  freeifaddrs(ifaddr);
  return ret;
}

auto main(int argc, char** argv) -> signed {
  //   size_t use_remote_flush = argv[1][0] - '0';
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  std::string db_name = "/tmp/rrtest/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.max_background_jobs = 16;
  opt.max_background_flushes = 3;
  opt.max_background_compactions = 12;
  opt.max_compaction_bytes = 1 << 30;
  opt.server_remote_flush = 0;
  opt.write_buffer_size = 64 << 10;

  opt.max_write_buffer_number = 4;
  opt.delayed_write_rate = 1 << 30;
  // opt.min_write_buffer_number_to_merge = 2;
  DB::Open(opt, db_name, &db);
  assert(db != nullptr);
  WriteOptions wo;
  auto* cf_impl =
      reinterpret_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily());
  int num = 0;
  cin >> num;
  for (int i = 10000; i >= num; i--) {
    db->Put(wo, std::to_string(i), std::to_string(i));
  }
  cf_impl->cfd()->mem()->TESTContinuous();
  db->Close();
  return 0;
}
