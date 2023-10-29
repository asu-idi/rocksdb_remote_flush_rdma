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

rocksdb::RDMAClient* client;
struct rocksdb::RDMANode::rdma_connection* conn;

uint64_t Get(uint32_t key){
	size_t req_ofs = client->rdma_mem_.allocate(sizeof(rocksdb::GetRequest));
	auto req = (rocksdb::GetRequest*)(client->get_buf() + req_ofs);
	size_t res_ofs = client->rdma_mem_.allocate(sizeof(rocksdb::GetResponse));
	auto res = (rocksdb::GetResponse*)(client->get_buf() + res_ofs);

	req->lookup_key = key;

	uint64_t send_id = rocksdb::obtain_wr_id(), recv_id = rocksdb::obtain_wr_id();
	client->receive(conn, sizeof(rocksdb::GetResponse), res_ofs, recv_id);
	client->send(conn, sizeof(rocksdb::GetRequest), req_ofs, send_id);
	client->poll_completion(conn, send_id);
	client->poll_completion(conn, recv_id);

	rocksdb::GetResponse ret = *res;

	client->rdma_mem_.free(req_ofs);
	client->rdma_mem_.free(res_ofs);

	return ret.value;
}

int main(int argc, char** argv) {
	if (argc != 3) {
		fprintf(stderr, "Parameters: [server_ip] [port]\n");
		return 0;
	}
	std::string server_ip(argv[1]);
	u_int32_t connect_port = std::atoi(argv[2]);

	client = new rocksdb::RDMAClient();
	client->resources_create(MAX_PARALLEL_GET_PER_CLIENT * REQUEST_BUFFER_UNIT_SIZE);
	client->rdma_mem_.init(client->buf_size);
    conn = client->sock_connect(server_ip, connect_port);
	client->register_client_in_get_service_request(conn);

	std::vector<std::thread*> threads;
	std::vector<std::pair<uint32_t, uint64_t> > result;
	for(int i = 0; i < 5; i++){
		auto thrd = [&result, i] {
			int key_interval = 10000;
			for(int key = i * key_interval; key < (i + 1) * key_interval; key++){
				auto value = Get(key);
				result.push_back(std::make_pair(key, value));
			}
		};
		threads.push_back(new std::thread(thrd));
	}
	for (auto& thread : threads) thread->join();

	return 0;
}