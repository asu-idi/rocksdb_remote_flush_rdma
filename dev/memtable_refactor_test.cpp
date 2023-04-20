// #include <sys/ipc.h>
// #include <sys/shm.h>
// #include <sys/types.h>

// #include <algorithm>
// #include <chrono>
// #include <cmath>
// #include <cstdlib>
// #include <ctime>
// #include <fstream>
// #include <ios>
// #include <iostream>
// #include <thread>

// #include "db/memtable.h"
// #include "rocksdb/c.h"
// #include "rocksdb/db.h"
// #include "rocksdb/options.h"
// #include "rocksdb/slice_transform.h"
// #include "rocksdb/status.h"
// #include "rocksdb/utilities/options_type.h"
// #include "rocksdb/write_batch_base.h"
// #include "trace_replay/trace_replay.h"
// #include "util/logger.hpp"
// #include "utilities/merge_operators.h"

// #define ROOT_DIR "/data/rocksdb/"
// using namespace std;
// using namespace rocksdb;

// auto main() -> signed {
//   rocksdb::Env* env = rocksdb::Env::Default();
//   EnvOptions env_options;
//   DB* db = nullptr;
//   std::string db_name = std::string(ROOT_DIR) + "dev/db/";
//   Options opt;
//   opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
//   opt.create_if_missing = true;
//   DB::Open(opt, db_name, &db);

//   // create column family
//   //   ColumnFamilyHandle* cf = nullptr;
//   //   ColumnFamilyOptions cfo;
//   //   Status s = db->CreateColumnFamily(cfo, "new_cf", &cf);
//   //   assert(s.ok());
//   //   WriteOptions wo;
//   //   db->Put(wo, cf, "a", " const Slice &value");

//   db->Close();
//   return 0;
// }

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
  //   db->Put(WriteOptions(), cf, " const Slice& key", "const Slice& value");
  //   db->CXLFlush(FlushOptions(), cf);
  db->DropColumnFamily(cf);
  db->Close();
  return 0;
}