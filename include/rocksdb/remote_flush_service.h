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
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/status.h"

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
    for (auto it = mempool_.begin(); it != mempool_.end(); ++it) {
      if (it->first == addr) {
        ::free(addr);
        mempool_.erase(it);
        break;
      }
    }
  }
};
// a mempool, using registered buffer
class RDMAMemNode {
  std::set<std::pair<size_t, size_t>> mempool_;
  std::mutex mtx_;
  size_t buf_size_;

 public:
  RDMAMemNode() = default;
  ~RDMAMemNode() { mempool_.clear(); }
  void init(size_t buf_size) { buf_size_ = buf_size; }
  size_t allocate(size_t size) {
    while (true) {
      std::lock_guard<std::mutex> lock(mtx_);
      bool flag = false;
      size_t offset = 0;
      if (mempool_.empty()) {
        if (size <= buf_size_)
          offset = 0, flag = true;
        else {
          fprintf(stderr, "Memory node does not have enough capacity\n");
          return -1;
        }
      } else {
        if (size <= mempool_.begin()->first)
          offset = 0, flag = true;
        else
          for (auto i = mempool_.begin(); i != mempool_.end(); i++) {
            auto j = i;
            j++;
            if (i->first + i->second + size <=
                (j != mempool_.end() ? j->first : buf_size_)) {
              offset = i->first + i->second, flag = true;
              break;
            }
          }
      }
      if (flag) {
        mempool_.insert(std::make_pair(offset, size));
        break;
      }
      return offset;
    }
  }
  void free(size_t offset) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = mempool_.lower_bound(std::make_pair(offset, 0));
    assert(it != mempool_.end());
    assert(it->first == offset);
    mempool_.erase(it);
  }
};

// tcp node, use in flush_job_server & worker & memnode.
// use sockfd to exchange data with peer
class TCPNode {
 public:
  explicit TCPNode(sockaddr_in client_addr, int32_t client_sockfd)
      : connection_info_{client_addr, client_sockfd, 0} {}
  ~TCPNode() {
    if (connection_info_.listen_sockfd > 0)
      close(connection_info_.listen_sockfd);
    if (connection_info_.client_sockfd > 0)
      close(connection_info_.client_sockfd);
  }
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
  friend class RDMAServer;
  friend class RDMAClient;
  friend class RemoteFlushJob;
  friend class DBImpl;
  // structure of test parameters
  struct config_t {
    std::string dev_name;  // IB device name
    int ib_port;           // local IB port to work with
    int gid_idx;           // gid index to use
    int max_cqe, max_send_wr, max_recv_wr;
  };
  // structure to exchange data which is needed to connect the QPs
  struct cm_con_data_t {
    uint64_t addr;    // Buffer address
    uint32_t rkey;    // Remote key
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
  } __attribute__((packed));
  struct rdma_connection {
    struct ibv_cq *cq;  // CQ handle
    struct ibv_qp *qp;  // QP handle
    int sock;           // TCP socket file descriptor
  };
  // structure of system resources
  struct resources {
    struct ibv_device_attr device_attr;  // Device attributes
    struct ibv_port_attr port_attr;      // IB port attributes
    struct cm_con_data_t remote_props;   // values to connect to remote side
    struct ibv_context *ib_ctx;          // device handle
    struct ibv_pd *pd;                   // PD handle
    std::vector<struct rdma_connection *> conns;
    struct ibv_mr *mr;  // MR handle for buf
    char *buf;          // memory buffer pointer, used for RDMA and send ops
  };

 private:
  struct rdma_connection *connect_qp(int sock);
  int sock_sync_data(int sock, int xfer_size, const char *local_data,
                     char *remote_data);
  int post_send(struct rdma_connection *idx, size_t msg_size,
                ibv_wr_opcode opcode, long long local_offset,
                long long remote_offset);
  int post_receive(struct rdma_connection *idx, size_t msg_size,
                   long long local_offset);
  int modify_qp_to_init(struct ibv_qp *qp);
  int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid,
                       uint8_t *dgid);
  int modify_qp_to_rts(struct ibv_qp *qp);
  int resources_destroy();
  virtual void after_connect_qp(struct rdma_connection *idx) = 0;
  std::unique_ptr<std::mutex> mtx;

 public:
  RDMANode();
  virtual ~RDMANode() = 0;
  int resources_create(size_t size);
  std::vector<struct rdma_connection *> sock_connect(
      const std::string &server_name = "", u_int32_t tcp_port = 9091);
  int send(struct rdma_connection *idx, size_t msg_size,
           long long local_offset) {
    return post_send(idx, msg_size, IBV_WR_SEND, local_offset, 0);
  }
  int receive(struct rdma_connection *idx, size_t msg_size,
              long long local_offset) {
    return post_receive(idx, msg_size, local_offset);
  }
  int rdma_read(struct rdma_connection *idx, size_t msg_size,
                long long local_offset, long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_READ, local_offset,
                     remote_offset);
  }
  int rdma_write(struct rdma_connection *idx, size_t msg_size,
                 long long local_offset, long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_WRITE, local_offset,
                     remote_offset);
  }
  int poll_completion(struct rdma_connection *idx);
  char *get_buf() { return res->buf; }
  struct resources *res;
  size_t buf_size;
  struct config_t config;
};

class RDMAServer : public RDMANode {
  struct executor_info {
    int status;
    std::queue<std::pair<size_t, size_t>> flush_job_queue;
    std::pair<size_t, size_t> current_job;
  };

 public:
  RDMAServer();
  ~RDMAServer();
  bool service(struct rdma_connection *idx);

 private:
  void allocate_mem_service(struct rdma_connection *idx);
  void modify_mem_service(struct rdma_connection *idx);
  void disconnect_service(struct rdma_connection *idx);
  void register_executor_service(struct rdma_connection *idx);
  void wait_for_job_service(struct rdma_connection *idx);
  std::map<std::pair<size_t, size_t>, int> mem_seg;
  std::vector<std::thread *> threads;
  std::unordered_map<struct rdma_connection *, executor_info> executors_;
  void after_connect_qp(struct rdma_connection *idx) override {
    auto ser = [this, idx] {
      while (true)
        if (!this->service(idx)) break;
    };
    threads.push_back(new std::thread(ser));
    threads.back()->detach();
  }
};

class RDMAClient : public RDMANode {
 public:
  RDMAClient();
  ~RDMAClient();
  std::pair<long long, long long> allocate_mem_request(
      struct rdma_connection *idx, size_t size);
  // type == 0: free; type == 1: accessible to generator; type == 2: accessible
  // to worker.
  bool modify_mem_request(struct rdma_connection *idx,
                          std::pair<long long, long long> offset, int type);
  bool disconnect_request(struct rdma_connection *idx);
  bool register_executor_request(struct rdma_connection *idx);
  std::pair<long long, long long> wait_for_job_request(
      struct rdma_connection *idx);
  size_t port = -1;
  RegularMemNode memory_;
  RDMAMemNode rdma_mem_;
  std::map<rdma_connection *, std::mutex> conn_mtx_;

 private:
  void after_connect_qp(struct rdma_connection *idx) override {}
};

struct placement_info {
  int current_background_job_num_ = 0;
  int current_hdfs_io_ = 0;
};

// PD listen on port 10086, receive FlushRequest from generator, receive
// HeartBeat from worker
struct PlacementDriver {
  const int max_background_job_num_{32};
  const int max_hdfs_io_{100 << 20};  // MB/s
  std::queue<TCPNode *> available_workers_;
  std::vector<TCPNode *> workers_;
  std::vector<TCPNode *> generators_;
  std::unordered_map<TCPNode *, placement_info> peers_;
  TCPNode *connect_peers(const std::string &ip, int port);
  TCPNode *choose_worker(const placement_info &);
  void step(bool, size_t, placement_info);
  void listen();
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
    if (flush_job_generators_.find(fd) != flush_job_generators_.end())
      assert(false);
    flush_job_generators_.insert(
        std::make_pair(fd, const_cast<TCPNode *>(node)));
  }
  TCPNode *unregister_flush_job_generator(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    assert(flush_job_generators_.find(fd) != flush_job_generators_.end());
    TCPNode *node = flush_job_generators_.at(fd);
    flush_job_generators_.erase(fd);
    return node;
  }
  void register_flush_job_executor([[maybe_unused]] const std::string &ip,
                                   int port) {
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
  void pd_add_worker(const std::string &ip, int port) {
    TCPNode *node = pd_.connect_peers(ip, port);
    assert(node != nullptr);
    size_t size = pd_.workers_.size() + 1;
    node->send(&size, sizeof(size_t));
    pd_.workers_.push_back(node);
  }
  void pd_add_generator(const std::string &ip, int port) {
    TCPNode *node = pd_.connect_peers(ip, port);
    assert(node != nullptr);
    size_t size = pd_.generators_.size() + 1;
    node->send(&size, sizeof(size_t));
    pd_.generators_.push_back(node);
  }

 private:
  struct flushjob_package {
    std::vector<std::pair<void *, size_t>> package;
  };
  flushjob_package *receive_remote_flush_job(TCPNode *generator_node);
  TCPNode *choose_flush_job_executor();
  void setfree_flush_job_executor(TCPNode *worker_node);
  void send_remote_flush_job(flushjob_package *package, TCPNode *worker_node);

  std::mutex mtx_;
  std::unordered_map<int, TCPNode *> flush_job_executors_in_use_;
  std::unordered_map<TCPNode *, bool> flush_job_executors_status_;
  std::unordered_map<int, TCPNode *> flush_job_generators_;
  PlacementDriver pd_;
  struct tcp_server_info {
    int tcp_server_sockfd_;
    struct sockaddr_in server_address;
  } __attribute__((aligned)) server_info_ = {-1};

 private:
  RemoteFlushJobPD() = default;
  ~RemoteFlushJobPD() {}
};

class PDClient {
 public:
  TCPNode *pd_connection_{nullptr};

 private:
  mutable std::mutex pd_mutex_;
  size_t peer_id_{0};
  int heartbeatport_{10086};
  std::function<placement_info()> get_placement_info{
      []() { return placement_info(); }};

 public:
  explicit PDClient(int heartbeatport) : heartbeatport_(heartbeatport) {}
  inline std::mutex &get_mutex() const { return pd_mutex_; }
  inline void set_get_placement_info(
      const std::function<placement_info()> &func) {
    get_placement_info = func;
  }
  inline Status MatchMemnodeForHeartBeat() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return Status::IOError("socket error");
    }
    sockaddr_in addr_;
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(heartbeatport_);
    addr_.sin_addr.s_addr = INADDR_ANY;
    printf("rocksdb local_heartbeatport_ %d\n", heartbeatport_);

    assert(bind(sock, (sockaddr *)&addr_, sizeof(addr_)) >= 0);
    assert(listen(sock, 1) >= 0);

    struct sockaddr_in client_address;
    socklen_t client_address_len = sizeof(client_address);
    printf("rocksdb before accept\n");
    int client_sockfd =
        accept(sock, (struct sockaddr *)&client_address, &client_address_len);
    printf("rocksdb accept\n");
    if (client_sockfd < 0) {
      return Status::IOError("accept error");
    }
    {
      char client_ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
      int client_port = ntohs(client_address.sin_port);
      LOG_CERR("Rocksdb Instance create connection with memnode: ", client_ip,
               ':', client_port);
    }
    auto *node = new TCPNode(client_address, client_sockfd);
    pd_connection_ = node;
    pd_connection_->receive(&peer_id_, sizeof(size_t));
    return Status::OK();
  }
  inline Status SendHeartBeat(const placement_info &pinfo) {
    {
      std::lock_guard<std::mutex> lock(pd_mutex_);
      if (pd_connection_ == nullptr) {
        assert(false);
        return Status::IOError("pd_connection_ is nullptr");
      }
      pd_connection_->send(&pinfo, sizeof(placement_info));
    }
    return Status::OK();
  }
  inline void match_memnode_for_request() {
    Status s = MatchMemnodeForHeartBeat();
    assert(s.ok());
  }
  inline void match_memnode_for_heartbeat() {
    Status s = MatchMemnodeForHeartBeat();
    assert(s.ok());
    std::thread heartbeat_thread([this]() {
      placement_info lastpinfo;
      while (true) {
        placement_info pinfo = get_placement_info();
        if (lastpinfo.current_background_job_num_ !=
                pinfo.current_background_job_num_ ||
            lastpinfo.current_hdfs_io_ != pinfo.current_hdfs_io_) {
          lastpinfo = pinfo;
          Status s0 = SendHeartBeat(pinfo);
          if (!s0.ok()) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    });
    heartbeat_thread.detach();
  }
  inline void register_local_heartbeat_port(int port = 10086) {
    heartbeatport_ = port;
  }
};

}  // namespace ROCKSDB_NAMESPACE