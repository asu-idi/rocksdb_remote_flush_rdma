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

rocksdb::RDMAServer server;

int main(int argc, char** argv) {
	if (argc != 1) {
		fprintf(stderr, "Parameters: \n");
		return 0;
	}
	size_t mem_size = MAX_NUMBER_OF_PARALLEL_GET_CLIENTS * MAX_PARALLEL_GET_PER_CLIENT * REQUEST_BUFFER_UNIT_SIZE;
	server.resources_create(mem_size);
	server.rdma_mem_.init(server.buf_size);
	server.sock_connect();
	return 0;
}