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

signed main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db_primary = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db_primary);

  // op
  WriteOptions wo;
  std::pair<std::string, std::string> kv;
  kv.first = std::to_string(1);
  for (int count = 0; count <= 200; count++) {
    int srand = rand() % 26;
    kv.second.push_back('a' + srand);
  }
  LOG("wb-put: ", kv.first, " ", kv.second);
  db_primary->Put(wo, kv.first, kv.second);

  assert(db_primary);

  //   Options options;
  //   options.max_open_files = -1;
  //   // Secondary instance needs its own directory to store info logs (LOG)
  //   const std::string kSecondaryPath =
  //       std::string(ROOT_DIR) + "dev/rocksdb_secondary/";
  //   DB* db_secondary = nullptr;

  //   Status s =
  //       DB::OpenAsSecondary(options, db_name, kSecondaryPath, &db_secondary);
  //   assert(!s.ok() || db_secondary);
  //   s = db_secondary->TryCatchUpWithPrimary();
  //   assert(s.ok());
  //   std::string value;
  //   s = db_secondary->Get(ReadOptions(), "1", &value);
  //   LOG("CHECK second kv: 1 ", value);

  // Assume we have already opened a regular
  // whose database directory is kDbPath.
  DB* ro_db = nullptr;
  Options options;
  /* Open Readonly with default CF alone */
  auto s = DB::OpenForReadOnly(options, db_name, &ro_db);
  assert(s.ok() && ro_db);

  ReadOptions ropts;
  Iterator* iter1 = ro_db->NewIterator(ropts);
  iter1->SeekToFirst();
  auto key_cnt = 0;
  for (; iter1->Valid(); iter1->Next(), key_cnt++) {
    LOG("Kv:", iter1->key().data(), iter1->value().data());
  }
  LOG("read keys:", key_cnt);
  db_primary->Close();
  return 0;
}