#include "memory/remote_flush_service.h"

#include <arpa/inet.h>
#include <byteswap.h>
#include <endian.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdint.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>
#include <utility>

#include "util/logger.hpp"

#define MAX_POLL_CQ_TIMEOUT 2000
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

namespace ROCKSDB_NAMESPACE {

bool TCPNode::send(const void *buf, size_t size) {
  size_t total = 0;
  const char *hheader = "Header";
  void *header_ = malloc(sizeof(size) + 6);
  memcpy(header_, hheader, 6);
  memcpy(reinterpret_cast<char *>(header_) + 6, &size, sizeof(size));
  assert(write(connection_info_.client_sockfd, header_, sizeof(size) + 6) ==
         sizeof(size) + 6);

  char *buf_ = reinterpret_cast<char *>(const_cast<void *>(buf));
  while (total < size) {
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(connection_info_.client_sockfd, &write_fds);
    int ret = select(connection_info_.client_sockfd + 1, nullptr, &write_fds,
                     nullptr, nullptr);
    if (ret == -1) {
      LOG("TCPNode::send: select error");
      assert(false);
    } else if (ret == 0) {
      continue;
    }
    ssize_t n =
        write(connection_info_.client_sockfd, buf_ + total, size - total);
    if (n == -1) {
      LOG("TCPNode::send: write error");
      assert(false);
    }
    total += n;
  }
  assert(total == size);
  return true;
}

// recv(buf!=nullptr,size!=0) => receive size bytes data to specific address
// recv(buf==nullptr,size==0) => receive n bytes data to new allocated address
bool TCPNode::receive(void **buf, size_t *size) {
  char *buf_ = reinterpret_cast<char *>(*buf);
  void *header_ = malloc(sizeof(size_t) + 6);
  ssize_t temp =
      read(connection_info_.client_sockfd, header_, sizeof(size_t) + 6);
  LOG("TCPNode::receive: read header:", temp, ' ',
      reinterpret_cast<char *>(header_), ' ',
      *reinterpret_cast<size_t *>(reinterpret_cast<char *>(header_) + 6));

  assert(memcmp(header_, "Header", 6) == 0);
  size_t package_size = 0;
  memcpy(&package_size, reinterpret_cast<char *>(header_) + 6,
         sizeof(package_size));
  free(header_);
  if (*size == 0)
    *size = package_size;
  else
    assert(package_size == *size);

  if (buf_ == nullptr) {
    buf_ = reinterpret_cast<char *>(memory_.allocate(package_size));
    *buf = buf_;
  }
  LOG("TCPNode::receive: package size:", package_size);
  size_t total = 0;
  while (total < package_size) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(connection_info_.client_sockfd, &read_fds);
    int ret = select(connection_info_.client_sockfd + 1, &read_fds, nullptr,
                     nullptr, nullptr);
    if (ret == -1) {
      LOG("TCPNode::receive: select error");
      assert(false);
    } else if (ret == 0) {
      continue;
    }

    ssize_t n = read(connection_info_.client_sockfd, buf_ + total,
                     package_size - total);
    if (n == -1) {
      LOG("TCPNode::receive: read error");
      assert(false);
    } else if (n == 0) {
      break;
    }
    total += n;
  }
  assert(total == package_size);
  return true;
}

bool RemoteFlushJobPD::closetcp() {
  if (server_info_.tcp_server_sockfd_ == -1) {
    LOG("tcp server not opened");
    return false;
  }
  assert(false);
  return true;
}

bool RemoteFlushJobPD::opentcp(int port) {
  if (server_info_.tcp_server_sockfd_ != -1) {
    LOG("tcp server already opened");
    return false;
  }
  int opt = ~SOCK_NONBLOCK;  // debug
  server_info_.tcp_server_sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  assert(server_info_.tcp_server_sockfd_ != -1);
  setsockopt(server_info_.tcp_server_sockfd_, SOL_SOCKET, SO_REUSEADDR, &opt,
             sizeof(opt));
  server_info_.server_address.sin_family = AF_INET;
  server_info_.server_address.sin_addr.s_addr = htonl(INADDR_ANY);
  server_info_.server_address.sin_port = htons(port);
  assert(bind(server_info_.tcp_server_sockfd_,
              (struct sockaddr *)&server_info_.server_address,
              sizeof(server_info_.server_address)) >= 0);
  assert(listen(server_info_.tcp_server_sockfd_, 10) >= 0);
  while (true) {
    struct sockaddr_in client_address;
    socklen_t client_address_len = sizeof(client_address);
    int client_sockfd =
        accept(server_info_.tcp_server_sockfd_,
               (struct sockaddr *)&client_address, &client_address_len);
    if (client_sockfd < 0) {
      LOG("tcp server accept error");
      continue;
    }
    {
      char client_ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
      int client_port = ntohs(client_address.sin_port);
      LOG_CERR("MemNode receive package from: ", client_ip, ':', client_port);
    }
    auto *node = new TCPNode(client_address, client_sockfd);
    register_flush_job_generator(client_sockfd, node);
    LOG("tcp server accept success");
    std::thread([client_sockfd, this]() {
      // BGworkRemoteFlush
      LOG("remote flush job generator connected. start receiving.");
      flushjob_package *package = receive_remote_flush_job(client_sockfd);
      LOG("remote flush job received from generator.");
      TCPNode *worker_tcpnode = choose_flush_job_executor();
      assert(worker_tcpnode != nullptr);
      LOG("remote flush job executor chosen.");
      send_remote_flush_job(package, worker_tcpnode);
      LOG("remote flush job sent to worker.");
      setfree_flush_job_executor(worker_tcpnode);
      LOG("remote flush job executor set free.");
      unregister_flush_job_generator(client_sockfd);
      LOG("remote flush job generator unregistered.");
    }).detach();
  }
  return true;
}

RemoteFlushJobPD::flushjob_package *RemoteFlushJobPD::receive_remote_flush_job(
    int client_sockfd) {
  auto tcpnode = flush_job_generators_.at(client_sockfd);
  auto *package = new flushjob_package();
  const char *bye = "byebyemessage";
  while (true) {
    void *buf_ = nullptr;
    size_t size = 0;
    tcpnode->receive(&buf_, &size);
    assert(buf_ != nullptr);
    if (size == 0) assert(false);
    LOG("memnode recv data from generator:", reinterpret_cast<char *>(buf_),
        ' ', size);
    if (size == strlen(bye) &&
        strncmp(reinterpret_cast<char *>(buf_), bye, size) == 0) {
      break;
    }
    package->package.push_back(std::make_pair(buf_, size));
  }
  LOG("receive_remote_flush_job: receive bye message");
  close(client_sockfd);
  return package;
}

void RemoteFlushJobPD::send_remote_flush_job(flushjob_package *package,
                                             TCPNode *worker_node) {
  for (auto &it : package->package) {
    LOG("remote flushjob worker send data:",
        *reinterpret_cast<size_t *>(it.first), ' ', it.second);
    worker_node->send(it.first, it.second);
  }
  worker_node->send("byebyemessage", strlen("byebyemessage"));
  close(worker_node->connection_info_.client_sockfd);
}

void RemoteFlushJobPD::setfree_flush_job_executor(TCPNode *worker_node) {
  std::lock_guard<std::mutex> lock(mtx_);
  flush_job_executors_status_.at(worker_node) = true;
  flush_job_executors_in_use_.erase(
      worker_node->connection_info_.client_sockfd);
  worker_node->connection_info_.client_sockfd = {};
}
TCPNode *RemoteFlushJobPD::choose_flush_job_executor() {
  std::lock_guard<std::mutex> lock(mtx_);
  for (auto &it : flush_job_executors_status_) {
    if (it.second) {
      it.second = false;
      int client_sockfd = socket(AF_INET, SOCK_STREAM, 0);
      assert(client_sockfd != -1);
      if (connect(client_sockfd,
                  reinterpret_cast<struct sockaddr *>(
                      &it.first->connection_info_.sin_addr),
                  sizeof(it.first->connection_info_.sin_addr)) < 0) {
        LOG("remote flushjob worker connect error");
        assert(false);
      }
      {
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &it.first->connection_info_.sin_addr.sin_addr,
                  client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(it.first->connection_info_.sin_addr.sin_port);
        LOG_CERR("MemNode send package to worker: ", client_ip, ':',
                 client_port);
      }
      it.first->connection_info_.client_sockfd = client_sockfd;
      flush_job_executors_in_use_.insert(
          std::make_pair(client_sockfd, it.first));
      return it.first;
    }
  }
  LOG("no available worker");
  return nullptr;
}

RDMANode::RDMANode() {
  config = (config_t){
      "",    // dev_name
      1,     // ib_port
      -1, // gid_idx
      10, 5, 5
  };
  res = new resources();
}

RDMANode::~RDMANode(){
	resources_destroy();
	delete res;
}

bool RDMANode::sock_connect(const std::string& server_name, u_int32_t tcp_port){
	const char* servername = server_name != "" ? server_name.c_str() : nullptr;
	int port = tcp_port;
  struct addrinfo *resolved_addr = nullptr;
  struct addrinfo *iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  // Resolve DNS address, use sockfd as temp storage
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }
  // Search through results and find the one we want
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        // Client mode. Initiate connection to remote
        if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          // LOG("failed connect \n");
          close(sockfd);
          sockfd = -1;
        } else {
					res->sock.push_back(sockfd);
					if (connect_qp())
						res->sock.pop_back();
          else
            after_connect_qp(res->sock.size() - 1);
				}
      } else {
        // Server mode. Set up listening socket an accept a connection
        listenfd = sockfd;
        sockfd = -1;
        if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
          goto sock_connect_exit;
        listen(listenfd, 1);
				while(true){
					sockfd = accept(listenfd, nullptr, 0);
					if(sockfd >= 0){
						res->sock.push_back(sockfd);
						if (connect_qp())
							res->sock.pop_back();
						else
							after_connect_qp(res->sock.size() - 1);
					}
        }
      }
    }
  }
sock_connect_exit:
  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
	if(res->sock.size() <= 0)
		if (servername)
			fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		else {
			perror("server accept");
			fprintf(stderr, "accept() failed\n");
		}
	return res->sock.size() > 0;
}

int RDMANode::sock_sync_data(int sock, int xfer_size, const char *local_data,
                             char *remote_data) {
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr, "Failed writing data during sock_sync_data\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < xfer_size) {
    read_bytes = read(sock, remote_data + total_read_bytes,
                      xfer_size - total_read_bytes);

    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  return rc;
}

int RDMANode::poll_completion(int idx) {
  struct ibv_wc wc;
  unsigned long start_time_msec;
  unsigned long cur_time_msec;
  struct timeval cur_time;
  int poll_result;
  int rc = 0;
  // poll the completion for a while before giving up of doing it ..
  gettimeofday(&cur_time, NULL);
  start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  do {
    poll_result = ibv_poll_cq(res->cq[idx], 1, &wc);
    gettimeofday(&cur_time, NULL);
    cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  } while ((poll_result == 0) &&
           ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  if (poll_result < 0) {
    // poll CQ failed
    fprintf(stderr, "poll CQ failed\n");
    rc = 1;
  } else if (poll_result == 0) {  // the CQ is empty
    fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
    rc = 1;
  } else {
    // CQE found
    // LOG("completion was found in CQ with status 0x%x\n", wc.status);
    // check the completion status (here we don't care about the completion
    // opcode
    if (wc.status != IBV_WC_SUCCESS) {
      fprintf(stderr,
              "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
              wc.status, wc.vendor_err);
      rc = 1;
    }
  }
  return rc;
}
int RDMANode::post_send(int idx, size_t msg_size, ibv_wr_opcode opcode,
                        long long local_offset, long long remote_offset) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr *bad_wr = nullptr;
  int rc = 0;
  // prepare the scatter/gather entry
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)res->buf + local_offset;
  sge.length = msg_size;
  sge.lkey = res->mr->lkey;
  // prepare the send work request
  memset(&sr, 0, sizeof(sr));
  sr.next = nullptr;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = opcode;
  sr.send_flags = IBV_SEND_SIGNALED;
  if (opcode != IBV_WR_SEND) {
    sr.wr.rdma.remote_addr = res->remote_props.addr + remote_offset;
    sr.wr.rdma.rkey = res->remote_props.rkey;
  }
  // there is a Receive Request in the responder side, so we won't get any into
  // RNR flow
  rc = ibv_post_send(res->qp[idx], &sr, &bad_wr);
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    switch (opcode) {
      case IBV_WR_SEND:
        // LOG("Send Request was posted\n");
        break;
      case IBV_WR_RDMA_READ:
        // LOG("RDMA Read Request was posted\n");
        break;
      case IBV_WR_RDMA_WRITE:
        // LOG("RDMA Write Request was posted\n");
        break;
      default:
        // LOG("Unknown Request was posted\n");
        break;
    }
  }
  return rc;
}

int RDMANode::post_receive(int idx, size_t msg_size, long long local_offset) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr *bad_wr;
  int rc = 0;
  // prepare the scatter/gather entry
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)res->buf + local_offset;
  sge.length = msg_size;
  sge.lkey = res->mr->lkey;
  // prepare the receive work request
  memset(&rr, 0, sizeof(rr));
  rr.next = nullptr;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  // post the Receive Request to the RQ
  rc = ibv_post_recv(res->qp[idx], &rr, &bad_wr);
  if (rc) fprintf(stderr, "failed to post RR\n");
  // else
  // 	LOG("Receive Request was posted\n");
  return rc;
}

int RDMANode::resources_create(size_t size){
	struct ibv_device **dev_list = nullptr;
	struct ibv_device *ib_dev = nullptr;
	int i;
	int mr_flags = 0;
	int num_devices;
	int rc = 0;
	// LOG("TCP connection was established\n");
  // LOG("searching for IB devices in host\n");
  // get device names in the system
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "failed to get IB devices list\n");
    rc = 1;
    goto resources_create_exit;
  }
  // if there isn't any IB device in host
  if (!num_devices) {
    fprintf(stderr, "found %d device(s)\n", num_devices);
    rc = 1;
    goto resources_create_exit;
  }
  // LOG("found %d device(s)\n", num_devices);
  // search for the specific device we want to work with
  for (i = 0; i < (size_t)num_devices; i++) {
    if (config.dev_name == "") {
      config.dev_name = std::string(strdup(ibv_get_device_name(dev_list[i])));
      // LOG("device not specified, using first one found: %s\n",
      // config.dev_name.c_str());
    }
    if (config.dev_name == ibv_get_device_name(dev_list[i])) {
      ib_dev = dev_list[i];
      break;
    }
  }
  // if the device wasn't found in host
  if (!ib_dev) {
    fprintf(stderr, "IB device %s wasn't found\n", config.dev_name.c_str());
    rc = 1;
    goto resources_create_exit;
  }
  // get device handle
  res->ib_ctx = ibv_open_device(ib_dev);
  if (!res->ib_ctx) {
    fprintf(stderr, "failed to open device %s\n", config.dev_name.c_str());
    rc = 1;
    goto resources_create_exit;
  }
  // We are now done with device list, free it
  ibv_free_device_list(dev_list);
  dev_list = nullptr;
  ib_dev = nullptr;
  // query port properties
  if (ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr)) {
    fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
    rc = 1;
    goto resources_create_exit;
  }
  // allocate Protection Domain
  res->pd = ibv_alloc_pd(res->ib_ctx);
  if (!res->pd) {
    fprintf(stderr, "ibv_alloc_pd failed\n");
    rc = 1;
    goto resources_create_exit;
  }
  // allocate the memory buffer that will hold the data
  buf_size = size;
  res->buf = new char[buf_size]();
  // register the memory buffer
  mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
  if (!res->mr) {
    fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
    rc = 1;
    goto resources_create_exit;
  }
  // LOG("MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
  // 		res->buf, res->mr->lkey, res->mr->rkey, mr_flags);
resources_create_exit:
  if (rc) {
    // Error encountered, cleanup
    if (res->mr) {
      ibv_dereg_mr(res->mr);
      res->mr = nullptr;
    }
    if (res->buf) {
      free(res->buf);
      res->buf = nullptr;
    }
    if (res->pd) {
      ibv_dealloc_pd(res->pd);
      res->pd = nullptr;
    }
    if (res->ib_ctx) {
      ibv_close_device(res->ib_ctx);
      res->ib_ctx = nullptr;
    }
    if (dev_list) {
      ibv_free_device_list(dev_list);
      dev_list = nullptr;
    }
    for (auto &sock : res->sock)
      if (sock >= 0) {
        if (close(sock)) fprintf(stderr, "failed to close socket\n");
        sock = -1;
      }
    res->sock.clear();
  }
  return rc;
}

int RDMANode::connect_qp(){
	int rc = 0;
	res->cq.push_back(nullptr);
	res->qp.push_back(nullptr);
	struct ibv_qp_init_attr qp_init_attr;
	// each side will send only one WR, so Completion Queue with 1 entry is enough
	res->cq.back() = ibv_create_cq(res->ib_ctx, config.max_cqe, nullptr, nullptr, 0);
	if (!res->cq.back()) {
		fprintf(stderr, "failed to create CQ with %u entries\n", config.max_cqe);
		rc = 1;
		goto connect_qp_exit;
	}
	// create the Queue Pair
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));
	qp_init_attr.qp_type = IBV_QPT_RC;
	qp_init_attr.sq_sig_all = 1;
	qp_init_attr.send_cq = res->cq.back();
	qp_init_attr.recv_cq = res->cq.back();
	qp_init_attr.cap.max_send_wr = config.max_send_wr;
	qp_init_attr.cap.max_recv_wr = config.max_recv_wr;
	qp_init_attr.cap.max_send_sge = 1;
	qp_init_attr.cap.max_recv_sge = 1;
	res->qp.back() = ibv_create_qp(res->pd, &qp_init_attr);
	if (!res->qp.back()) {
		fprintf(stderr, "failed to create QP\n");
		rc = 1;
		goto connect_qp_exit;
	}
	// LOG("QP was created, QP number=0x%x\n", res->qp->qp_num);

  struct cm_con_data_t local_con_data;
  struct cm_con_data_t remote_con_data;
  struct cm_con_data_t tmp_con_data;
  char temp_char;
  union ibv_gid my_gid;
  if (config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              config.ib_port, config.gid_idx);
			goto connect_qp_exit;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  // exchange using TCP sockets info required to connect QPs
  local_con_data.addr = htonll((uintptr_t)res->buf);
  local_con_data.rkey = htonl(res->mr->rkey);
	local_con_data.qp_num = htonl(res->qp.back()->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  // LOG("\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(res->sock.back(), sizeof(struct cm_con_data_t),
                     (char *)&local_con_data, (char *)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
    goto connect_qp_exit;
  }
  remote_con_data.addr = ntohll(tmp_con_data.addr);
  remote_con_data.rkey = ntohl(tmp_con_data.rkey);
  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data.lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
  // save the remote side attributes, we will need it for the post SR
  res->remote_props = remote_con_data;
  // LOG("Remote address = ", static_cast<unsigned long
  // long>(remote_con_data.addr), "\n"); LOG("Remote rkey = ",
  // static_cast<unsigned long long>(remote_con_data.rkey), "\n"); LOG("Remote
  // QP number = ", static_cast<unsigned long long>(remote_con_data.qp_num),
  // "\n"); LOG("Remote LID = ", static_cast<unsigned long
  // long>(remote_con_data.lid), "\n");
  if (config.gid_idx >= 0) {
    uint8_t *p = remote_con_data.gid;
    // LOG("Remote GID =", p[0], ":", p[1], ":", p[2], ":", p[3], ":", p[4],
    // ":", p[5], ":", p[6], ":", p[7], ":", p[8],
    // 	":", p[9], ":", p[10], ":", p[11], ":", p[12], ":", p[13], ":", p[14],
    // ":", p[15], "\n");
  }
  // modify the QP to init
	rc = modify_qp_to_init(res->qp.back());
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }
  // modify the QP to RTR
  rc = modify_qp_to_rtr(res->qp.back(), remote_con_data.qp_num,
                        remote_con_data.lid, remote_con_data.gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(res->qp.back());
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  // LOG("QP state was change to RTS\n");
  // sync to make sure that both sides are in states that they can connect to
  // prevent packet loose
  if (sock_sync_data(res->sock.back(), 1, "Q",
                     &temp_char)) {  // just send a dummy char back and forth
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
  }
connect_qp_exit:
	if (rc) {
		if (res->qp.back())
			ibv_destroy_qp(res->qp.back());
		res->qp.pop_back();
		if (res->cq.back())
			ibv_destroy_cq(res->cq.back());
		res->cq.pop_back();
	}
  return rc;
}

int RDMANode::resources_destroy() {
  int rc = 0;
  for (auto &qp : res->qp)
    if (qp)
      if (ibv_destroy_qp(qp)) {
        fprintf(stderr, "failed to destroy QP\n");
        rc = 1;
      }
  res->qp.clear();
  if (res->mr)
    if (ibv_dereg_mr(res->mr)) {
      fprintf(stderr, "failed to deregister MR\n");
      rc = 1;
    }
  if (res->buf) free(res->buf);
  for (auto &cq : res->cq)
    if (cq)
      if (ibv_destroy_cq(cq)) {
        fprintf(stderr, "failed to destroy CQ\n");
        rc = 1;
      }
  res->cq.clear();
  if (res->pd)
    if (ibv_dealloc_pd(res->pd)) {
      fprintf(stderr, "failed to deallocate PD\n");
      rc = 1;
    }
  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx)) {
      fprintf(stderr, "failed to close device context\n");
      rc = 1;
    }
  for (auto &sock : res->sock)
    if (sock >= 0)
      if (close(sock)) {
        fprintf(stderr, "failed to close socket\n");
        rc = 1;
      }
  res->sock.clear();
  return rc;
}

int RDMANode::modify_qp_to_init(struct ibv_qp *qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc = 0;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to INIT\n");
  return rc;
}
int RDMANode::modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn,
                               uint16_t dlid, uint8_t *dgid) {
  struct ibv_qp_attr attr;
  int flags;
  int rc = 0;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_256;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 0x12;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = config.ib_port;
  if (config.gid_idx >= 0) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = config.gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTR\n");
  return rc;
}
int RDMANode::modify_qp_to_rts(struct ibv_qp *qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc = 0;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0x12;
  attr.retry_cnt = 6;
  attr.rnr_retry = 0;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 1;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTS\n");
  return rc;
}

RDMAServer::RDMAServer() : RDMANode() { mtx = std::make_unique<std::mutex>(); }
RDMAClient::RDMAClient() : RDMANode() {}
std::pair<long long, long long> RDMAClient::allocate_mem_request(int idx,
                                                                 size_t size) {
  char req_type = 1;
  long long ret[2];
  int local_size = sizeof(size_t), remote_size = sizeof(long long) * 2;
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&req_type), sizeof(char));
  if (rc < (int)(sizeof(char)))
    fprintf(stderr, "Failed writing data during allocate_mem_request\n");
  else
    rc = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&size), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during allocate_mem_request\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(ret) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  return std::make_pair(ret[0], ret[1]);
}
void RDMAServer::allocate_mem_service(int idx) {
  size_t size;
  long long ret[2];
  int remote_size = sizeof(size_t), local_size = sizeof(long long) * 2;
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(&size) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  while (true) {
    std::lock_guard<std::mutex> lk(*mtx);
    bool flag = false;
    if (mem_seg.empty()) {
      if (size <= buf_size)
        ret[0] = 0, ret[1] = size, flag = true;
      else {
        fprintf(stderr, "Memory node does not have enough capacity\n");
        return;
      }
    } else {
      if (size <= mem_seg.begin()->first.first)
        ret[0] = 0, ret[1] = size, flag = true;
      else
        for (auto i = mem_seg.begin(); i != mem_seg.end(); i++) {
          auto j = i;
          j++;
          if (i->first.second + size <=
              (j != mem_seg.end() ? j->first.first : buf_size)) {
            ret[0] = i->first.second, ret[1] = i->first.second + size,
            flag = true;
            break;
          }
        }
    }
    if (flag) {
      mem_seg[std::make_pair(ret[0], ret[1])] = 1;
      break;
    }
  }
  rc = write(res->sock[idx], reinterpret_cast<void *>(ret), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during allocate_mem_service\n");
  else
    rc = 0;
}
bool RDMAClient::modify_mem_request(int idx,
                                    std::pair<long long, long long> offset,
                                    int type) {
  char req_type = 2;
  long long tmp[3] = {offset.first, offset.second, type};
  bool ret = false;
  int local_size = sizeof(long long) * 3, remote_size = sizeof(bool);
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&req_type), sizeof(char));
  if (rc < (int)sizeof(char))
    fprintf(stderr, "Failed writing data during modify_mem_request\n");
  else
    rc = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(tmp), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during modify_mem_request\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(&ret) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  return ret;
}
void RDMAServer::modify_mem_service(int idx) {
  long long input[3];
  bool ret = false;
  int remote_size = sizeof(long long) * 3, local_size = sizeof(bool);
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(input) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  {
    std::lock_guard<std::mutex> lk(*mtx);
    auto iter = mem_seg.find(std::make_pair(input[0], input[1]));
    if (iter == mem_seg.end()) {
      ret = false;
      fprintf(stderr, "Memory node cannot find memory segment\n");
    } else if (input[2] == 0 && iter->second == 2) {
      for (auto &it : executors_) {
        if (!it.second.status && it.second.current_job == iter->first) {
          ret = true;
          mem_seg.erase(iter);
          it.second.status = true;
          break;
        }
      }
    } else if ((input[2] == 2 && iter->second == 1)) {
      for (auto &it : executors_) {
        if (it.second.status) {
          ret = true;
          iter->second = input[2];
          it.second.status = false;
          it.second.flush_job_queue.push(iter->first);
          break;
        }
      }
    } else {
      ret = false;
      fprintf(stderr, "Unexpected memory segment state\n");
    }
  }
  rc = write(res->sock[idx], reinterpret_cast<void *>(&ret), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during modify_mem_service\n");
  else
    rc = 0;
}
bool RDMAClient::disconnect_request(int idx) {
  char req_type = 0;
  bool ret = false;
  int remote_size = sizeof(bool);
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&req_type), sizeof(char));
  if (rc < (int)sizeof(char))
    fprintf(stderr, "Failed writing data during disconnect_request\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(&ret) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  if (ret) {
		if (res->qp[idx])
			ibv_destroy_qp(res->qp[idx]);
		res->qp[idx] = nullptr;
		if (res->cq[idx])
			ibv_destroy_cq(res->cq[idx]);
		res->cq[idx] = nullptr;
    if (res->sock[idx] >= 0) {
      if (close(res->sock[idx])) fprintf(stderr, "failed to close socket\n");
      res->sock[idx] = -1;
    }
  }
  return ret;
}
void RDMAServer::disconnect_service(int idx) {
  bool ret = true;
  int local_size = sizeof(bool);
  int rc = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&ret), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during disconnect_service\n");
  else
    rc = 0;
  if (res->qp[idx])
    ibv_destroy_qp(res->qp[idx]);
  res->qp[idx] = nullptr;
  if (res->cq[idx])
    ibv_destroy_cq(res->cq[idx]);
  res->cq[idx] = nullptr;
  if (res->sock[idx] >= 0) {
    if (close(res->sock[idx])) fprintf(stderr, "failed to close socket\n");
    res->sock[idx] = -1;
  }
}
bool RDMAClient::register_executor_request(int idx) {
  char req_type = 3;
  bool ret = false;
  int remote_size = sizeof(bool);
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&req_type), sizeof(char));
  if (rc < (int)sizeof(char))
    fprintf(stderr, "Failed writing data during register_executor_request\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(&ret) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  return ret;
}
void RDMAServer::register_executor_service(int idx) {
  bool ret = true;
  int local_size = sizeof(bool);
  int rc = 0;
  std::lock_guard<std::mutex> lk(*mtx);
  executors_[idx].status = true;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&ret), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during register_executor_service\n");
  else
    rc = 0;
}
std::pair<long long, long long> RDMAClient::wait_for_job_request(int idx) {
  char req_type = 4;
  long long ret[2];
  int remote_size = sizeof(long long) * 2;
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(res->sock[idx], reinterpret_cast<void *>(&req_type), sizeof(char));
  if (rc < (int)sizeof(char))
    fprintf(stderr, "Failed writing data during wait_for_job_request\n");
  else
    rc = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes =
        read(res->sock[idx], reinterpret_cast<char *>(&ret) + total_read_bytes,
             remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  return std::make_pair(ret[0], ret[1]);
}
void RDMAServer::wait_for_job_service(int idx) {
  long long ret[2];
  int local_size = sizeof(long long) * 2;
  int rc = 0;
  while(true){
    std::lock_guard<std::mutex> lk(*mtx);
    if(!executors_[idx].flush_job_queue.empty()){
      ret[0] = executors_[idx].flush_job_queue.front().first;
      ret[1] = executors_[idx].flush_job_queue.front().second;
      executors_[idx].flush_job_queue.pop();
      break;
    }
  }
  rc = write(res->sock[idx], reinterpret_cast<void *>(&ret), local_size);
  if (rc < local_size)
    fprintf(stderr, "Failed writing data during wait_for_job_service\n");
  else
    rc = 0;
}
bool RDMAServer::service(int idx) {
  char req_type;
  int remote_size = sizeof(char);
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  while (!rc && total_read_bytes < remote_size) {
    read_bytes = read(res->sock[idx],
                      reinterpret_cast<char *>(&req_type) + total_read_bytes,
                      remote_size - total_read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
  switch (req_type) {
    case 0:
      disconnect_service(idx);
      return false;
    case 1:
      allocate_mem_service(idx);
      break;
    case 2:
      modify_mem_service(idx);
      break;
    case 3:
      register_executor_service(idx);
      break;
    case 4:
      wait_for_job_service(idx);
      break;
    default:
      fprintf(stderr, "Unknown request type from %d-th client: %d\n", idx,
              req_type);
  }
  return true;
}

}  // namespace ROCKSDB_NAMESPACE
