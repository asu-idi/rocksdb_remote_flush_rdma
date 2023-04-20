#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "util/logger.hpp"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;
// query trace Get, WriteBatch (Put, Delete, Merge, SingleDelete, and
// DeleteRange), Iterator (Seek and SeekForPrev) and MultiGet
auto main() -> signed {
  LOG("CHECK COMPILE");
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  std::string trace_path = std::string(ROOT_DIR) + "dev/trace";
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  /*Create the trace file writer*/
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  /*Start IO tracing*/
  std::unique_ptr<TraceWriter> trace_writer;
  auto ret = NewFileTraceWriter(env, env_options, trace_path, &trace_writer);
  assert(ret.code() == Status::kOk);
  assert(trace_writer != nullptr);
  TraceOptions trace_opt;

  db->StartTrace(trace_opt, std::move(trace_writer));
  // rocksdb ops
  WriteOptions wo;
  WriteBatch wb;

  LOG("Start Test Wb");
  std::chrono::steady_clock::time_point tp_begin =
      std::chrono::steady_clock::now();
  for (int i = 1;; i++) {
    std::pair<std::string, std::string> kv;
    kv.first = std::to_string(i);

    for (int count = 0; count <= 200; count++) {
      int srand = rand() % 26;
      kv.second.push_back('a' + srand);
    }
    LOG("wb-put: ", kv.first, " ", kv.second);
    wb.Put(kv.first, kv.second);
    db->Write(wo, &wb);
    wb.Clear();
    auto tp_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - tp_begin);

    if (tp_duration >= std::chrono::milliseconds(200)) {
      LOG("End Put Operation, total Put op count = ", i);
      break;
    }
  }
  tp_begin = std::chrono::steady_clock::now();
  ReadOptions ro;
  if (wb.HasMerge()) {
    LOG("writeBatch have merge op");
  } else {
    LOG("writeBatch don't have merge op");
  }
  for (int i = 1;; i++) {
    std::pair<std::string, std::string> kv;
    kv.first = std::to_string(i);

    for (int count = 0; count <= 200; count++) {
      int srand = rand() % 26;
      kv.second.push_back('a' + srand);
    }
    LOG("wb-merge: ", kv.first, kv.second);
    wb.Merge(kv.first, kv.second);
    db->Write(wo, &wb);
    wb.Clear();
    // string ret_val;
    // db->Get(ro, kv.first, &ret_val);
    // LOG("GET-op: ", kv.first, ret_val);
    auto tp_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - tp_begin);

    if (tp_duration >= std::chrono::milliseconds(200)) {
      LOG("End Merge Operation, total Merge op count = ", i);
      break;
    }
  }

  tp_begin = std::chrono::steady_clock::now();
  LOG("Start Test Get");
  for (int i = 1;; i++) {
    int rand_num = rand() % 2000 + 1;
    std::string val;
    db->Get(ro, std::to_string(rand_num), &val);
    LOG("GET op: ", rand_num, " ", val);
    auto tp_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - tp_begin);

    if (tp_duration >= std::chrono::milliseconds(200)) {
      LOG("End Merge Operation, total Merge op count = ", i);
      break;
    }
  }
  tp_begin = std::chrono::steady_clock::now();

  LOG("Start Test Iter Seek");
  auto iter = db->NewIterator(ro);
  iter->SeekForPrev("222");
  std::string retry_val;
  db->Get(ro, std::to_string(2222), &retry_val);
  LOG("reCHECK =", retry_val);
  while (iter->Valid()) {
    LOG("Iter-Seek op: ", iter->key().data(), " ", iter->value().data());
    iter->Next();
  }

  /*End IO tracing*/
  db->EndTrace();
  return 0;
}