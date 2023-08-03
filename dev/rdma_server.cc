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

#include "memory/remote_flush_service.h"

// TODO(rdma): need to receive different packages simutanously, and choose one
// registered worker to send package to it.
int main(int argc, char** argv) {
  if (argc > 4) {
    fprintf(stderr, "Parameters: [conn_cnt] [mem_size] [port]\n");
    return 0;
  }
  int conn_cnt = argc >= 2 ? std::atoi(argv[1]) : 2;
  size_t mem_size = argc >= 3 ? std::atoll(argv[2]) : 1ull << 28;
  rocksdb::RDMAServer server;
  // if(argc >= 4) server.config.tcp_port = std::atoi(argv[3]);
  server.resources_create(mem_size, conn_cnt);
  for (int i = 0; i < conn_cnt; i++) server.connect_qp(i);
  std::vector<std::thread*> threads;
  for (int i = 0; i < conn_cnt; i++) {
    auto ser = [i, &server] {
      while (true) server.service(i);
    };
    threads.push_back(new std::thread(ser));
    threads.back()->detach();
  }
  while (true) {
    std::string command;
    std::cin >> command;
    if (command == "stop") break;
  }
  return 0;
}