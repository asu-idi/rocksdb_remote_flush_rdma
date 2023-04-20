#include <algorithm>
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
#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;

signed main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  ColumnFamilyOptions cfo;
  ColumnFamilyHandle* cf = nullptr;
  db->CreateColumnFamily(cfo, "cf_pre", &cf);
  db->Put(WriteOptions(), cf, "12345", "62345");
  db->Put(WriteOptions(), cf, "123456", "62345");
  LOG("begin flush, thread_id=", std::this_thread::get_id());
  db->Flush(FlushOptions(), cf);
  LOG("finish flush");
  db->DropColumnFamily(cf);
  db->Close();
  return 0;
}