#include <ifaddrs.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "db/memtable.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/logger.hpp"
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
  size_t use_remote_flush = argv[1][0] - '0';
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  // TODO(rdma): change this on your machine
  std::string db_name = "/tmp/rrtest/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  // opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  opt.max_background_flushes = 32;
  opt.server_remote_flush = use_remote_flush;
  opt.write_buffer_size = 64 << 20;

  // opt.max_background_compactions = 0;
  // opt.disable_auto_compactions = true;

  opt.max_write_buffer_number = 4;
  opt.delayed_write_rate = 100 << 20;
  // opt.min_write_buffer_number_to_merge = 2;
  DB::Open(opt, db_name, &db);
  assert(db != nullptr);

  // TODO(rdma): register memnodes ip&port, or other information that rdma needs
  // here
  if (opt.server_remote_flush) {
    db->register_memnode("127.0.0.1", 9091);
    // db->register_memnode("127.0.0.1", 9092);
  }
  std::string local_ip = find_local_ip();
  db->register_local_ip(local_ip);
  LOG("local_ip: ", local_ip);

  std::thread t[10];
  for (size_t i = 0; i < 10; i++) {
    t[i] = std::thread([i, db]() {
      std::map<std::string, std::string> kv_pairs;
      int cnt_miss = 0;
      ColumnFamilyOptions cfo;
      // cfo.write_buffer_size = 64 << 20;
      // cfo.disable_auto_compactions = true;
      ColumnFamilyHandle* cf = nullptr;
      db->CreateColumnFamily(cfo, "cf_num_" + std::to_string(i), &cf);
      for (size_t j = 0; j < 1000; j++) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int64_t> dis(0, 1000000000000000000);
        std::string key = std::to_string(dis(gen));
        std::string value = std::to_string(dis(gen));
        if (kv_pairs.find(key) != kv_pairs.end()) {
          continue;
        }
        LOG("put, key=", key, " value=", value);
        db->Put(WriteOptions(), cf, key, value);
        kv_pairs.insert(std::make_pair(key, value));
      }
      db->Flush(FlushOptions(), cf);
      cnt_miss = 0;
      for (auto kv : kv_pairs) {
        std::string value;
        db->Get(ReadOptions(), cf, kv.first, &value);
        LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second,
            "\"");
        if (value != kv.second) cnt_miss++;
      }
      cout << "cnt_miss=" << cnt_miss << endl;
      db->DisableFileDeletions();
      db->DropColumnFamily(cf);
      db->DestroyColumnFamilyHandle(cf);
    });
  }

  std::map<std::string, std::string> kv_pairs;
  int cnt_miss = 0;
  for (size_t j = 0; j < 1000; j++) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, 1000000000000000000);
    std::string key = std::to_string(dis(gen));
    std::string value = std::to_string(dis(gen));
    if (kv_pairs.find(key) != kv_pairs.end()) {
      continue;
    }
    LOG("put, key=", key, " value=", value);
    db->Put(WriteOptions(), key, value);
    kv_pairs.insert(std::make_pair(key, value));
  }
  db->Flush(FlushOptions());
  cnt_miss = 0;
  for (auto kv : kv_pairs) {
    std::string value;
    db->Get(ReadOptions(), kv.first, &value);
    LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second, "\"");
    if (value != kv.second) cnt_miss++;
  }
  cout << "cnt_miss=" << cnt_miss << endl;
  db->DisableFileDeletions();
  for (auto& i : t) {
    i.join();
  }
  // int all_cf[] = {114514, 114515, 19119, 909090909, 233333333};
  // ColumnFamilyOptions cfo;
  // cfo.write_buffer_size = 1 << 20;
  // cfo.disable_auto_compactions = true;
  // ColumnFamilyHandle* cf = nullptr;
  // db->CreateColumnFamily(cfo, "cf_pre", &cf);
  // for (size_t i = 0; i < 10000000; i++) {
  //   std::random_device rd;
  //   std::mt19937 gen(rd());
  //   std::uniform_int_distribution<int64_t> dis(0, 1000000000000000000);
  //   std::string key = std::to_string(dis(gen));
  //   std::string value = std::to_string(dis(gen));
  //   if (kv_pairs.find(key) != kv_pairs.end()) {
  //     continue;
  //   }
  //   LOG("put, key=", key, " value=", value);
  //   db->Put(WriteOptions(), cf, key, value);
  //   kv_pairs.insert(std::make_pair(key, value));
  // }
  // db->Flush(FlushOptions(), cf);
  // cnt_miss = 0;
  // for (auto kv : kv_pairs) {
  //   std::string value;
  //   db->Get(ReadOptions(), cf, kv.first, &value);
  //   LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second, "\"");
  //   if (value != kv.second) cnt_miss++;
  // }
  // cout << "cnt_miss=" << cnt_miss << endl;
  // db->DisableFileDeletions();
  // db->DropColumnFamily(cf);
  // db->DestroyColumnFamilyHandle(cf);
  db->Close();
  return 0;
}