//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <deque>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

#define PACK_TO_BUF(src, buf, len) \
  {                                \
    memcpy((buf), (src), (len));   \
    (buf) += (len);                \
  }
#define UNPACK_FROM_BUF(buf, des, len) \
  {                                    \
    memcpy((des), (buf), (len));       \
    (buf) += (len);                    \
  }

class RDMANode {
  // structure of test parameters
  struct config_t {
    std::string dev_name;     // IB device name
    std::string server_name;  // server host name
    u_int32_t tcp_port;       // server TCP port
    int ib_port;              // local IB port to work with
    int gid_idx;              // gid index to use
  };
  // structure to exchange data which is needed to connect the QPs
  struct cm_con_data_t {
    uint64_t addr;    // Buffer address
    uint32_t rkey;    // Remote key
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
  } __attribute__((packed));
  // structure of system resources
  struct resources {
    struct ibv_device_attr device_attr;  // Device attributes
    struct ibv_port_attr port_attr;      // IB port attributes
    struct cm_con_data_t remote_props;   // values to connect to remote side
    struct ibv_context *ib_ctx;          // device handle
    struct ibv_pd *pd;                   // PD handle
    std::vector<struct ibv_cq *> cq;     // CQ handle
    std::vector<struct ibv_qp *> qp;     // QP handle
    struct ibv_mr *mr;                   // MR handle for buf
    char *buf;              // memory buffer pointer, used for RDMA and send ops
    std::vector<int> sock;  // TCP socket file descriptor
  };

 private:
  std::vector<int> sock_connect(const char *servername, int port,
                                size_t conn_cnt);
  int sock_sync_data(int sock, int xfer_size, const char *local_data,
                     char *remote_data);
  int post_send(int idx, size_t msg_size, ibv_wr_opcode opcode,
                long long local_offset, long long remote_offset);
  int post_receive(int idx, size_t msg_size, long long local_offset);
  int modify_qp_to_init(struct ibv_qp *qp);
  int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid,
                       uint8_t *dgid);
  int modify_qp_to_rts(struct ibv_qp *qp);
  int resources_destroy();

 public:
  RDMANode();
  ~RDMANode() = default;
  int resources_create(size_t size, size_t conn_cnt = 1, size_t max_wr = 5);
  int connect_qp(int idx);
  int send(int idx, size_t msg_size, long long local_offset) {
    return post_send(idx, msg_size, IBV_WR_SEND, local_offset, 0);
  }
  int receive(int idx, size_t msg_size, long long local_offset) {
    return post_receive(idx, msg_size, local_offset);
  }
  int rdma_read(int idx, size_t msg_size, long long local_offset,
                long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_READ, local_offset,
                     remote_offset);
  }
  int rdma_write(int idx, size_t msg_size, long long local_offset,
                 long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_WRITE, local_offset,
                     remote_offset);
  }
  int poll_completion(int idx);
  char *get_buf() { return res->buf; }
  struct resources *res;
  size_t buf_size;
  struct config_t config;
};

class RDMAServer : public RDMANode {
 public:
  RDMAServer();
  void service(int idx);

 private:
  void allocate_mem_service(int idx);
  void modify_mem_service(int idx);
  std::map<std::pair<size_t, size_t>, int> mem_seg;
  std::unique_ptr<std::mutex> mtx;
};

class RDMAClient : public RDMANode {
 public:
  RDMAClient();
  std::pair<long long, long long> allocate_mem_request(int idx, size_t size);
  // type == 0: free; type == 1: being written; type == 2: occupied; type == 3:
  // being read.
  bool modify_mem_request(int idx, std::pair<long long, long long> offset,
                          int type);
};

}  // namespace ROCKSDB_NAMESPACE