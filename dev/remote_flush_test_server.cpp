#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
#include <random>
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

auto main(int argc, char** argv) -> signed {
  size_t use_remote_flush = argv[1][0] - '0';
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  std::string db_name = "/tmp/rrtest/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  // opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  opt.max_background_flushes = 32;
  opt.server_remote_flush = use_remote_flush;
  opt.write_buffer_size = 1024 * 1024;
  opt.max_write_buffer_number = 4;
  opt.min_write_buffer_number_to_merge = 2;
  DB::Open(opt, db_name, &db);
  assert(db != nullptr);
  // ColumnFamilyOptions cfo;
  // ColumnFamilyHandle* cf = nullptr;

  // db->CreateColumnFamily(cfo, "cf_pre", &cf);
  std::map<std::string, std::string> kv_pairs;
  int all[5] = {10101010, 21212121, 32323232, 43434343, 54545454};
  for (size_t i = 0; i < 5; i++) {
    std::random_device rd;
    // std::mt19937 gen(rd());
    // std::uniform_int_distribution<> dis(0, 100000000);
    std::string key = std::to_string(i);
    std::string value = std::to_string(all[i]);
    if (kv_pairs.find(key) != kv_pairs.end()) {
      continue;
    }
    LOG("put, key=", key, " value=", value);
    db->Put(WriteOptions(), key, value);
    kv_pairs.insert(std::make_pair(key, value));
  }
  std::this_thread::sleep_for(std::chrono::seconds(2));
  LOG("begin Flush, thread_id=", std::this_thread::get_id());
  db->Flush(FlushOptions());
  LOG("finish flush, thread_id=", std::this_thread::get_id());
  int cnt_miss = 0;
  for (auto kv : kv_pairs) {
    std::string value;
    db->Get(ReadOptions(), kv.first, &value);
    LOG("get, key=", kv.first, " value=\"", value, "\" \"", kv.second, "\"");
    if (value != kv.second) {
      cnt_miss++;
    }
  }
  cout << "cnt_miss=" << cnt_miss << endl;
  //   LOG("begin flush, thread_id=", std::this_thread::get_id());
  //   db->Flush(FlushOptions(), cf);
  //   LOG("finish flush");
  //   db->DropColumnFamily(cf);
  // db->DisableFileDeletions();
  // db->Close();
  // LOG("db closed first");
  // DB::Open(opt, db_name, &db);

  db->DisableFileDeletions();
  db->Close();
  return 0;
}