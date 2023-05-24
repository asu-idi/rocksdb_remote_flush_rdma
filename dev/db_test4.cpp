#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
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

int main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.server_use_remote_flush = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  ColumnFamilyOptions cfo;
  ColumnFamilyHandle* cf = nullptr;
  db->CreateColumnFamily(cfo, "cf_pre", &cf);
  db->Put(WriteOptions(), cf, "12345", "62345");
  db->Put(WriteOptions(), cf, "123456", "623455");
  LOG("begin flush, thread_id=", std::this_thread::get_id());
  db->Flush(FlushOptions(), cf);
  LOG("finish flush");
  ReadOptions ro;
  std::string ret_val;
  db->Get(ro, cf, Slice("12345"), &ret_val);
  assert(ret_val == "62345");
  db->Get(ro, cf, Slice("123456"), &ret_val);
  assert(ret_val == "623455");
  db->DropColumnFamily(cf);
  db->Close();
  return 0;
}