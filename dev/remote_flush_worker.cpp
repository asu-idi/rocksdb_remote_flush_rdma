#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
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
#include "util/cast_util.h"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/root/code/rocksdb_remote_flush/"
using namespace std;
using namespace rocksdb;

signed main(signed argc, char** argv) {
  if (argc != 4 && argc != 5) {
    std::cout << "Usage: " << argv[0]
              << "[memnode_ip] [memnode_port] [local_listen_port] "
                 "[memnode_heartbeat_port (default 10086)]"
              << std::endl;
    return -1;
  }
  std::string memnode_ip = argv[1];
  int memnode_port = std::atoi(argv[2]);
  int local_listen_port = std::atoi(argv[3]);
  int memnode_heartbeat_port = (argc == 5) ? std::atoi(argv[4]) : 10086;
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db" +
                        std::to_string(local_listen_port) + "/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  opt.max_background_flushes = 32;
  opt.max_background_jobs = 32;

  DB::Open(opt, db_name, &db);
  assert(db != nullptr);

  db->register_memnode(memnode_ip, memnode_port, 0);

  PDClient pd_client{memnode_heartbeat_port};
  pd_client.match_memnode_for_heartbeat(
      memnode_ip);  // waiting for any memnode to match
  db->register_pd_client(&pd_client);

  Status ret = db->ListenAndScheduleFlushJob(local_listen_port);
  assert(ret.ok());
  db->Close();
  delete db;
  return 0;
}