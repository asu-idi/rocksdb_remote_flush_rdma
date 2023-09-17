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

#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
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
  opt.max_background_jobs = 16;
  opt.max_background_flushes = 3;
  opt.max_background_compactions = 12;
  opt.max_compaction_bytes = 1 << 30;
  opt.server_remote_flush = use_remote_flush;
  opt.write_buffer_size = 1 << 20;

  opt.max_write_buffer_number = 4;
  opt.delayed_write_rate = 1 << 30;
  // opt.min_write_buffer_number_to_merge = 2;
  auto* pd_client = new PDClient{10089};
  pd_client->match_memnode_for_request();
  DB::Open(opt, db_name, &db);
  assert(db != nullptr);

  // TODO(rdma): register memnodes ip&port, or other information that rdma needs
  // here
  std::string local_ip = find_local_ip();
  db->register_local_ip(local_ip);
  LOG("local_ip: ", local_ip);
  if (opt.server_remote_flush) {
    db->register_memnode("127.0.0.1", 9091);
    db->register_pd_client(pd_client);
    printf("pd_client registered\n");
  }

  [[maybe_unused]] std::chrono::steady_clock::time_point all_begin =
      std::chrono::steady_clock::now();
  std::thread t[15];
  for (size_t i = 0; i < 15; i++) {
    t[i] = std::thread([i, db]() {
      std::this_thread::sleep_for(std::chrono::seconds(i * 2));
      [[maybe_unused]] std::chrono::steady_clock::time_point begin =
          std::chrono::steady_clock::now();
      // std::map<std::string, std::string> kv_pairs;
      int cnt_miss = 0;
      ColumnFamilyOptions cfo;

      ColumnFamilyHandle* cf = nullptr;
      assert(db->CreateColumnFamily(cfo, "cf_num_" + std::to_string(i), &cf) ==
             Status::OK());
      Status ret = Status::OK();
      for (size_t j = 0; j < 750000000; j++) {
        WriteOptions wo;
        wo.disableWAL = true;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int64_t> dis(0, 1000000000000000000);
        std::string key = std::to_string(dis(gen));
        std::string value = std::to_string(dis(gen));
        if (j % 50000 == 0) {
          [[maybe_unused]] std::chrono::steady_clock::time_point end =
              std::chrono::steady_clock::now();
          LOG_CERR(
              "cfd:", cf->GetName(), " write kv pairs: ", j, "time:",
              std::chrono::duration_cast<std::chrono::milliseconds>(end - begin)
                  .count(),
              "ms", " speed:",
              j * 1000.0 /
                  std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                        begin)
                      .count(),
              " kvp/s");
        }

        // if (kv_pairs.find(key) != kv_pairs.end()) {
        //   continue;
        // }
        // LOG("put, key=", key, " value=", value);
        ret = db->Put(wo, cf, key, value);
        // assert(ret == Status::OK());
        // kv_pairs.insert(std::make_pair(key, value));
      }
      db->Flush(FlushOptions(), cf);
      // cnt_miss = 0;
      // for (auto kv : kv_pairs) {
      //   std::string value;
      //   db->Get(ReadOptions(), cf, kv.first, &value);
      //   LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second,
      //       "\"");
      //   if (value != kv.second) cnt_miss++;
      // }
      // cout << "cfd_name" << i << " cnt_miss=" << cnt_miss << endl;
      db->DisableFileDeletions();
      db->DropColumnFamily(cf);
      db->DestroyColumnFamilyHandle(cf);
    });
  }

  // std::map<std::string, std::string> kv_pairs;
  int cnt_miss = 0;
  WriteOptions wo;
  wo.disableWAL = true;
  //  40Bytes kv pair, 30GB, 750000000 kv pairs
  for (size_t j = 0; j < 750000000; j++) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(0, 1000000000000000000);
    std::string key = std::to_string(dis(gen)) + std::to_string(j);
    std::string value = std::to_string(dis(gen));
    // if (kv_pairs.find(key) != kv_pairs.end()) {
    //   continue;
    // }
    LOG("put, key=", key, " value=", value);
    db->Put(wo, key, value);
    // kv_pairs.insert(std::make_pair(key, value));
  }
  db->Flush(FlushOptions());
  // cnt_miss = 0;
  // for (auto kv : kv_pairs) {
  //   std::string value;
  //   db->Get(ReadOptions(), kv.first, &value);
  //   LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second, "\"");
  //   if (value != kv.second) cnt_miss++;
  // }
  // cout << " default cnt_miss=" << cnt_miss << endl;
  db->DisableFileDeletions();
  for (auto& i : t) {
    i.join();
  }
  [[maybe_unused]] std::chrono::steady_clock::time_point all_end =
      std::chrono::steady_clock::now();
  LOG_CERR(
      "all write kv pairs: ", 750000000, "time:",
      std::chrono::duration_cast<std::chrono::milliseconds>(all_end - all_begin)
          .count(),
      "ms", " speed:",
      750000000 * 1000.0 /
          std::chrono::duration_cast<std::chrono::milliseconds>(all_end -
                                                                all_begin)
              .count(),
      " kvp/s");
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
