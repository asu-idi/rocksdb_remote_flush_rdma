//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <map>
#include <string>

#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/flush_job.h"
#include "db/remote_flush_job.h"
#include "db/version_set.h"
#include "file/writable_file_writer.h"
#include "memory/remote_flush_service.h"
#include "memory/shared_package.h"
#include "rocksdb/cache.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/mock_table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/logger.hpp"
#include "util/macro.hpp"
#include "util/random.h"
#include "util/string_util.h"

int main(int argc, char** argv) {
  if (argc < 2 || argc > 3) {
    fprintf(stderr, "Parameters: [port] [mem size]\n");
    return 0;
  }
  rocksdb::RDMANode server;
  server.config.tcp_port = std::atoi(argv[1]);
  server.resources_create(argc == 3 ? std::atoll(argv[2]) : 1ull << 27);
  server.connect_qp(1);
  while (true) {
    std::string command;
    std::cin >> command;
    if (command == "stop") break;
  }
  return 0;
}