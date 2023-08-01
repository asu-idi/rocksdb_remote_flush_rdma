//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include <iostream>
#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <string>
#include <arpa/inet.h>

#include "memory/remote_flush_service.h"

int main(int argc, char** argv) {
	if (argc > 4){
		fprintf(stderr, "Parameters: [conn_cnt] [mem_size] [port]\n");
		return 0;
	}
	int conn_cnt = argc >= 2 ? std::atoi(argv[1]) : 2;
	size_t mem_size = argc >= 3 ? std::atoll(argv[2]) : 1ull << 28;
	rocksdb::RDMAServer server;
	// if(argc >= 4) server.config.tcp_port = std::atoi(argv[3]);
	server.resources_create(mem_size, conn_cnt);
	server.connect_qp(0);
	server.connect_qp(1);
	while (true){
		std::string command;
		std::cin >> command;
		if (command == "stop")
			break;
	}
	return 0;
}