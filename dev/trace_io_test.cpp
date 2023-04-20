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
#include "util/logger.hpp"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;

signed main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  std::string trace_path = std::string(ROOT_DIR) + "dev/trace";
  std::string query_trace_path = std::string(ROOT_DIR) + "dev/trace1";
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  // io_tracer
  std::unique_ptr<TraceWriter> trace_writer;
  auto ret = NewFileTraceWriter(env, env_options, trace_path, &trace_writer);
  assert(ret.code() == Status::kOk);
  assert(trace_writer != nullptr);
  TraceOptions trace_opt;
  db->StartIOTrace(trace_opt, std::move(trace_writer));
  // query_tracer

  std::unique_ptr<TraceWriter> trace_writer1;
  auto ret1 =
      NewFileTraceWriter(env, env_options, query_trace_path, &trace_writer1);
  assert(ret1.code() == Status::kOk);
  assert(trace_writer1 != nullptr);
  TraceOptions trace_opt1;
  db->StartTrace(trace_opt1, std::move(trace_writer1));
  // operations
  WriteOptions wo;
  //   for (int i = 1;; i++) {
  std::pair<std::string, std::string> kv;
  for (int j = 1; j <= 50; j++) {
    char rand1 = rand() % 26 + 'a';
    char rand2 = rand() % 26 + 'a';
    kv.first.push_back(rand1);
    kv.second.push_back(rand2);
  }
  db->Put(wo, kv.first, kv.second);
  LOG("Start Test Wb");
  WriteBatch wb;
  std::chrono::steady_clock::time_point tp_begin =
      std::chrono::steady_clock::now();
  for (int i = 1;; i++) {
    std::pair<std::string, std::string> kvp;
    kvp.first = std::to_string(i);

    for (int count = 0; count <= 200; count++) {
      int srand = rand() % 26;
      kvp.second.push_back('a' + srand);
    }
    wb.Put(kvp.first, kvp.second);
    LOG("wb-put: ", kvp.first, " ", kvp.second);
    db->Write(wo, &wb);
    wb.Clear();
    auto tp_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - tp_begin);

    if (tp_duration >= std::chrono::milliseconds(5)) {
      LOG("End Put Operation, total Put op count = ", i);
      break;
    }
  }
  db->EndIOTrace();
  db->Close();
  return 0;
}