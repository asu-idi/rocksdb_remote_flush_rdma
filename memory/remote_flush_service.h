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
#include <linux/types.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util/thread_local.h"

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

// a mempool, using malloc/free to allocate/free memorys
class RegularMemNode {
  std::vector<std::pair<void *, size_t>> mempool_;
  std::mutex mtx_;

 public:
  RegularMemNode() = default;
  ~RegularMemNode() {
    for (auto &p : mempool_) {
      ::free(reinterpret_cast<char *>(p.first));
    }
    mempool_.clear();
  }
  char *allocate(size_t size) {
    std::lock_guard<std::mutex> lock(mtx_);
    char *addr = reinterpret_cast<char *>(malloc(size));
    mempool_.emplace_back(addr, size);
    return addr;
  }
  void free(char *addr) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = std::find_if(mempool_.begin(), mempool_.end(),
                           [addr](const std::pair<void *, size_t> &p) {
                             return p.first == reinterpret_cast<void *>(addr);
                           });
    if (it != mempool_.end())
      ::free(addr);
    else
      assert(false);
    mempool_.erase(it);
  }
};

// tcp node, use in flush_job_server & worker & memnode.
// use sockfd to exchange data with peer
class TCPNode {
 public:
  explicit TCPNode(sockaddr_in client_addr, int32_t client_sockfd)
      : connection_info_{client_addr, client_sockfd, 0} {}
  ~TCPNode() = default;
  bool send(const void *buf, size_t size);

  // set address & length to avoid unnecessary copy
  bool receive(void *buf, size_t size) {
    assert(buf != nullptr);
    return receive(&buf, size);
  }
  // set length, buffer will be allocated/released by TCPNode::mempool
  bool receive(void **buf, size_t size) {
    assert(size != 0);
    return receive(buf, &size);
  }
  // data length not sure
  bool receive(void **buf, size_t *size);

  struct tcp_connect_meta {
    struct sockaddr_in sin_addr;
    int32_t client_sockfd;
    int32_t listen_sockfd;
  } __attribute__((aligned)) connection_info_;

 private:
  RegularMemNode memory_;
};

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
  ~RDMANode() { delete res; }
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
  size_t port = -1;
  bool is_init_ = false;
  RegularMemNode memory_;
};

// register_workers then opentcp
class RemoteFlushJobPD {
 public:
  RemoteFlushJobPD(const RemoteFlushJobPD &) = delete;
  auto operator=(const RemoteFlushJobPD &) -> RemoteFlushJobPD & = delete;
  static RemoteFlushJobPD &Instance() {
    static RemoteFlushJobPD instance;
    return instance;
  }

 public:
  bool opentcp(int port);
  bool closetcp();
  void register_flush_job_generator(int fd, const TCPNode *node) {
    std::lock_guard<std::mutex> lock(mtx_);
    flush_job_generators_.insert(
        std::make_pair(fd, const_cast<TCPNode *>(node)));
  }
  void unregister_flush_job_generator(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    assert(flush_job_generators_.find(fd) != flush_job_generators_.end());
    TCPNode *node = flush_job_generators_.at(fd);
    flush_job_generators_.erase(fd);
    delete node;
  }
  void register_flush_job_executor(const std::string &ip, int port) {
    std::lock_guard<std::mutex> lock(mtx_);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    assert(inet_pton(AF_INET, ip.data(), &serv_addr.sin_addr) > 0);
    serv_addr.sin_port = htons(port);

    auto *node = new TCPNode(serv_addr, {});
    flush_job_executors_status_.insert(std::make_pair(node, true));
  }
  void unregister_flush_job_executor(TCPNode *node) {
    std::lock_guard<std::mutex> lock(mtx_);
    flush_job_executors_status_.erase(node);
    assert(flush_job_executors_in_use_.find(
               node->connection_info_.client_sockfd) ==
           flush_job_executors_in_use_.end());
    assert(node->connection_info_.client_sockfd <= 0);
    delete node;
  }

 private:
  struct flushjob_package {
    std::vector<std::pair<void *, size_t>> package;
  };
  flushjob_package *receive_remote_flush_job(int client_sockfd);
  TCPNode *choose_flush_job_executor();
  void setfree_flush_job_executor(TCPNode *worker_node);
  void send_remote_flush_job(flushjob_package *package, TCPNode *worker_node);

  std::mutex mtx_;
  std::unordered_map<int, TCPNode *> flush_job_executors_in_use_;
  std::unordered_map<TCPNode *, bool> flush_job_executors_status_;
  std::unordered_map<int, TCPNode *> flush_job_generators_;
  struct tcp_server_info {
    int tcp_server_sockfd_;
    struct sockaddr_in server_address;
  } __attribute__((aligned)) server_info_ = {-1};

 private:
  RemoteFlushJobPD() = default;
  ~RemoteFlushJobPD() {}
};

}  // namespace ROCKSDB_NAMESPACE