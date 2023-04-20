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
#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;

signed main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  std::string block_cache_trace_path = std::string(ROOT_DIR) + "dev/trace";
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  std::unique_ptr<TraceWriter> trace_writer;
  auto ret = NewFileTraceWriter(env, env_options, block_cache_trace_path,
                                &trace_writer);
  assert(ret.code() == Status::kOk);
  assert(trace_writer != nullptr);
  TraceOptions trace_opt;
  db->StartBlockCacheTrace(trace_opt, std::move(trace_writer));

  // op
  LOG("Start Test Wb");
  WriteBatch wb;
  WriteOptions wo;
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

    if (tp_duration >= std::chrono::seconds(200)) {
      LOG("End Put Operation, total Put op count = ", i);
      break;
    }
  }
  db->EndBlockCacheTrace();
  db->Close();
  return 0;
}