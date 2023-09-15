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
#include <thread>

#include "rocksdb/remote_flush_service.h"

// TODO(rdma): need to receive different packages simutanously, and choose one
// registered worker to send package to it.
int main(int argc, char** argv) {
  if (argc > 3) {
    fprintf(stderr, "Parameters: [mem_size] [port]\n");
    return 0;
  }
  size_t mem_size = argc >= 2 ? std::atoll(argv[1]) : 1ull << 28;
  rocksdb::RDMAServer server;
  // if(argc >= 3) server.config.tcp_port = std::atoi(argv[2]);
  server.pd_add_generator("10.145.21.36", 10089);
  server.pd_add_worker("10.145.21.34", 10087);
  server.connect_clients();
  server.resources_create(mem_size);
  server.sock_connect();
  while (true) {
    std::string command;
    std::cin >> command;
    if (command == "stop") break;
  }
  return 0;
}