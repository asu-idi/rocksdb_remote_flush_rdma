#include "memory/remote_flush_service.h"

#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace ROCKSDB_NAMESPACE{

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

RDMANode::RDMANode(){
	config = (config_t){
		"",  // dev_name
		"",  // server_name
		9876, // tcp_port
		1,	 // ib_port
		-1 // gid_idx
	};
	res = new resources();
}

RDMANode::~RDMANode(){
	resources_destroy();
	delete res;
}

std::vector<int> RDMANode::sock_connect(const char *servername, int port, size_t conn_cnt){
	std::vector<int> ret;
	struct addrinfo *resolved_addr = nullptr;
	struct addrinfo *iterator;
	char service[6];
	int sockfd = -1;
	int listenfd = 0;
	int tmp;
	struct addrinfo hints = {
		.ai_flags = AI_PASSIVE,
		.ai_family = AF_INET,
		.ai_socktype = SOCK_STREAM
	};
	if (sprintf(service, "%d", port) < 0)
		goto sock_connect_exit;
	// Resolve DNS address, use sockfd as temp storage
	sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
	if (sockfd < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
		goto sock_connect_exit;
	}
	// Search through results and find the one we want
	for (iterator = resolved_addr; iterator && conn_cnt; iterator = iterator->ai_next) {
		sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
		if (sockfd >= 0) {
			if (servername) {
				// Client mode. Initiate connection to remote
				if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
					// LOG("failed connect \n");
					close(sockfd);
					sockfd = -1;
				}
				else{
					conn_cnt--;
					ret.push_back(sockfd);
					continue;
				}
            }
			else {
				// Server mode. Set up listening socket an accept a connection
				listenfd = sockfd;
				sockfd = -1;
				if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
					goto sock_connect_exit;
				listen(listenfd, 1);
				sockfd = accept(listenfd, nullptr, 0);
				if(sockfd >= 0){
					conn_cnt--;
					ret.push_back(sockfd);
					continue;
				}
			}
		}
	}
sock_connect_exit:
	if (listenfd)
		close(listenfd);
	if (resolved_addr)
		freeaddrinfo(resolved_addr);
	for(auto& sock: ret)
		if (sock < 0) {
			if (servername)
				fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
			else {
				perror("server accept");
				fprintf(stderr, "accept() failed\n");
			}
		}
	return ret;
}

int RDMANode::sock_sync_data(int sock, int xfer_size, const char *local_data, char *remote_data){
	int rc;
	int read_bytes = 0;
	int total_read_bytes = 0;
	rc = write(sock, local_data, xfer_size);
	if (rc < xfer_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
	while (!rc && total_read_bytes < xfer_size) {
		read_bytes = read(sock, remote_data, xfer_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	return rc;
}

int RDMANode::poll_completion(int idx){
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
	} while ((poll_result == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
	if (poll_result < 0) {
		// poll CQ failed
		fprintf(stderr, "poll CQ failed\n");
		rc = 1;
	}
	else if (poll_result == 0) { // the CQ is empty
		fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
		rc = 1;
	}
	else {
		// CQE found
		// LOG("completion was found in CQ with status 0x%x\n", wc.status);
		// check the completion status (here we don't care about the completion opcode
		if (wc.status != IBV_WC_SUCCESS) {
			fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,
					wc.vendor_err);
			rc = 1;
		}
	}
	return rc;
}

int RDMANode::post_send(int idx, size_t msg_size, ibv_wr_opcode opcode, long long local_offset, long long remote_offset)
{
	struct ibv_send_wr sr;
	struct ibv_sge sge;
	struct ibv_send_wr *bad_wr = nullptr;
	int rc;
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
	// there is a Receive Request in the responder side, so we won't get any into RNR flow
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

int RDMANode::post_receive(int idx, size_t msg_size, long long local_offset){
	struct ibv_recv_wr rr;
	struct ibv_sge sge;
	struct ibv_recv_wr *bad_wr;
	int rc;
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
	if (rc)
		fprintf(stderr, "failed to post RR\n");
	// else
	// 	LOG("Receive Request was posted\n");
	return rc;
}

int RDMANode::resources_create(size_t size, size_t conn_cnt, size_t max_wr){
	struct ibv_device **dev_list = nullptr;
	struct ibv_qp_init_attr qp_init_attr;
	struct ibv_device *ib_dev = nullptr;
	int i;
	int mr_flags = 0;
	int cq_size = 0;
	int num_devices;
	int rc = 0;
	// if client side
	if (config.server_name != "") {
		res->sock = sock_connect(config.server_name.c_str(), config.tcp_port, conn_cnt);
		for(auto& sock: res->sock)
			if (sock < 0) {
				fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
						config.server_name.c_str(), config.tcp_port);
				rc = -1;
				goto resources_create_exit;
			}
	}
	else {
		// LOG("waiting on port %d for TCP connection\n", config.tcp_port);
		res->sock = sock_connect(nullptr, config.tcp_port, conn_cnt);
		for(auto& sock: res->sock)
			if (sock < 0) {
				fprintf(stderr, "failed to establish TCP connection with client on port %d\n",
						config.tcp_port);
				rc = -1;
				goto resources_create_exit;
			}
	}
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
	for (i = 0; i < num_devices; i++) {
		if (config.dev_name == "") {
			config.dev_name = std::string(strdup(ibv_get_device_name(dev_list[i])));
			// LOG("device not specified, using first one found: %s\n", config.dev_name.c_str());
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
	// each side will send only one WR, so Completion Queue with 1 entry is enough
	cq_size = 10;
	for(i = 0; i < conn_cnt; i++){
		res->cq.push_back(ibv_create_cq(res->ib_ctx, cq_size, nullptr, nullptr, 0));
		if (!res->cq.back()) {
			fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
			rc = 1;
			goto resources_create_exit;
		}
	}
	// allocate the memory buffer that will hold the data
	buf_size = size;
	res->buf = new char[buf_size]();
	// register the memory buffer
	mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
	if (!res->mr) {
		fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
		rc = 1;
		goto resources_create_exit;
	}
	// LOG("MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
	// 		res->buf, res->mr->lkey, res->mr->rkey, mr_flags);
	// create the Queue Pair
	res->qp.resize(conn_cnt);
	for(i = 0; i < conn_cnt; i++){
		memset(&qp_init_attr, 0, sizeof(qp_init_attr));
		qp_init_attr.qp_type = IBV_QPT_RC;
		qp_init_attr.sq_sig_all = 1;
		qp_init_attr.send_cq = res->cq[i];
		qp_init_attr.recv_cq = res->cq[i];
		qp_init_attr.cap.max_send_wr = max_wr;
		qp_init_attr.cap.max_recv_wr = max_wr;
		qp_init_attr.cap.max_send_sge = 1;
		qp_init_attr.cap.max_recv_sge = 1;
		res->qp[i] = ibv_create_qp(res->pd, &qp_init_attr);
		if (!res->qp[i]) {
			fprintf(stderr, "failed to create QP\n");
			rc = 1;
			goto resources_create_exit;
		}
		// LOG("QP was created, QP number=0x%x\n", res->qp->qp_num);
	}
resources_create_exit:
	if (rc) {
		// Error encountered, cleanup
		for(auto& qp: res->qp)
			if (qp) {
				ibv_destroy_qp(qp);
				qp = nullptr;
			}
		res->qp.clear();
		if (res->mr) {
			ibv_dereg_mr(res->mr);
			res->mr = nullptr;
		}
		if (res->buf) {
			free(res->buf);
			res->buf = nullptr;
		}
		for(auto& cq: res->cq)
			if (cq) {
				ibv_destroy_cq(cq);
				cq = nullptr;
			}
		res->cq.clear();
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
		for(auto& sock: res->sock)
			if (sock >= 0) {
				if (close(sock))
					fprintf(stderr, "failed to close socket\n");
				sock = -1;
			}
		res->sock.clear();
	}
	return rc;
}

int RDMANode::connect_qp(int idx){
	struct cm_con_data_t local_con_data;
	struct cm_con_data_t remote_con_data;
	struct cm_con_data_t tmp_con_data;
	int rc = 0;
	char temp_char;
	union ibv_gid my_gid;
	if (config.gid_idx >= 0) {
		rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
		if (rc) {
			fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
			return rc;
		}
	}
	else
		memset(&my_gid, 0, sizeof my_gid);
	// exchange using TCP sockets info required to connect QPs
	local_con_data.addr = htonll((uintptr_t)res->buf);
	local_con_data.rkey = htonl(res->mr->rkey);
	local_con_data.qp_num = htonl(res->qp[idx]->qp_num);
	local_con_data.lid = htons(res->port_attr.lid);
	memcpy(local_con_data.gid, &my_gid, 16);
	// LOG("\nLocal LID = 0x%x\n", res->port_attr.lid);
	if (sock_sync_data(res->sock[idx], sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0) {
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
	// LOG("Remote address = ", static_cast<unsigned long long>(remote_con_data.addr), "\n");
	// LOG("Remote rkey = ", static_cast<unsigned long long>(remote_con_data.rkey), "\n");
	// LOG("Remote QP number = ", static_cast<unsigned long long>(remote_con_data.qp_num), "\n");
	// LOG("Remote LID = ", static_cast<unsigned long long>(remote_con_data.lid), "\n");
	if (config.gid_idx >= 0) {
		uint8_t *p = remote_con_data.gid;
		// LOG("Remote GID =", p[0], ":", p[1], ":", p[2], ":", p[3], ":", p[4], ":", p[5], ":", p[6], ":", p[7], ":", p[8],
		// 	":", p[9], ":", p[10], ":", p[11], ":", p[12], ":", p[13], ":", p[14], ":", p[15], "\n");
	}
	// modify the QP to init
	rc = modify_qp_to_init(res->qp[idx]);
	if (rc) {
		fprintf(stderr, "change QP state to INIT failed\n");
		goto connect_qp_exit;
	}
	// modify the QP to RTR
	rc = modify_qp_to_rtr(res->qp[idx], remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid);
	if (rc) {
		fprintf(stderr, "failed to modify QP state to RTR\n");
		goto connect_qp_exit;
	}
	rc = modify_qp_to_rts(res->qp[idx]);
	if (rc) {
		fprintf(stderr, "failed to modify QP state to RTR\n");
		goto connect_qp_exit;
	}
	// LOG("QP state was change to RTS\n");
	// sync to make sure that both sides are in states that they can connect to prevent packet loose
	if (sock_sync_data(res->sock[idx], 1, "Q", &temp_char)) { // just send a dummy char back and forth
		fprintf(stderr, "sync error after QPs are were moved to RTS\n");
		rc = 1;
	}
connect_qp_exit:
	return rc;
}

int RDMANode::resources_destroy(){
	int rc = 0;
	for(auto& qp: res->qp)
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
	if (res->buf)
		free(res->buf);
	for(auto& cq: res->cq)
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
	for(auto& sock: res->sock)
		if (sock >= 0)
			if (close(sock)) {
				fprintf(stderr, "failed to close socket\n");
				rc = 1;
			}
	res->sock.clear();
	return rc;
}

int RDMANode::modify_qp_to_init(struct ibv_qp *qp){
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_INIT;
	attr.port_num = config.ib_port;
	attr.pkey_index = 0;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to INIT\n");
	return rc;
}
int RDMANode::modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid){
	struct ibv_qp_attr attr;
	int flags;
	int rc;
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
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTR\n");
	return rc;
}
int RDMANode::modify_qp_to_rts(struct ibv_qp *qp){
	struct ibv_qp_attr attr;
	int flags;
	int rc;
	memset(&attr, 0, sizeof(attr));
	attr.qp_state = IBV_QPS_RTS;
	attr.timeout = 0x12;
	attr.retry_cnt = 6;
	attr.rnr_retry = 0;
	attr.sq_psn = 0;
	attr.max_rd_atomic = 1;
	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
			IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc)
		fprintf(stderr, "failed to modify QP state to RTS\n");
	return rc;
}

RDMAServer::RDMAServer(): RDMANode(){
	mtx = std::make_unique<std::mutex>();
}
RDMAClient::RDMAClient(): RDMANode(){
}
std::pair<long long, long long> RDMAClient::allocate_mem_request(int idx, size_t size){
	long long ret[2];
	int local_size = sizeof(size_t), remote_size = sizeof(long long) * 2;
	int rc;
	int read_bytes = 0;
	int total_read_bytes = 0;
	rc = write(res->sock[idx], reinterpret_cast<void*>(&size), local_size);
	if (rc < local_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
	while (!rc && total_read_bytes < remote_size) {
		read_bytes = read(res->sock[idx], reinterpret_cast<void*>(ret), remote_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	return std::make_pair(ret[0], ret[1]);
}
void RDMAServer::allocate_mem_service(int idx){
	size_t size;
	long long ret[2];
	int remote_size = sizeof(size_t), local_size = sizeof(long long) * 2;
	int rc;
	int read_bytes = 0;
	int total_read_bytes = 0;
	while (!rc && total_read_bytes < remote_size) {
		read_bytes = read(res->sock[idx], reinterpret_cast<void*>(&size), remote_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	while(true){
		std::lock_guard<std::mutex> lk(*mtx);
		bool flag = false;
		if(mem_seg.empty()){
			if(size <= buf_size)
				ret[0] = 0, ret[1] = size, flag = true;
			else{
				fprintf(stderr, "Memory node does not have enough capacity\n");
				return;
			}
		}
		else{
			if(size <= mem_seg.begin()->first.first)
				ret[0] = 0, ret[1] = size, flag = true;
			else for(auto i = mem_seg.begin(); i != mem_seg.end(); i++){
				auto j = i;
				j++;
				if(i->first.second + size <= (j != mem_seg.end() ? j->first.first : buf_size)){
					ret[0] = i->first.second, ret[1] = i->first.second + size, flag = true;
					break;
				}
			}
		}
		if(flag){
			mem_seg[std::make_pair(ret[0], ret[1])] = 1;
			break;
		}
	}
	rc = write(res->sock[idx], reinterpret_cast<void*>(ret), local_size);
	if (rc < local_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
}
bool RDMAClient::modify_mem_request(int idx, std::pair<long long, long long> offset, int type){
	long long tmp[3] = {offset.first, offset.second, type};
	bool ret = false;
	int local_size = sizeof(long long) * 3, remote_size = sizeof(bool);
	int rc;
	int read_bytes = 0;
	int total_read_bytes = 0;
	rc = write(res->sock[idx], reinterpret_cast<void*>(tmp), local_size);
	if (rc < local_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
	while (!rc && total_read_bytes < remote_size) {
		read_bytes = read(res->sock[idx], reinterpret_cast<void*>(&ret), remote_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	return ret;
}
void RDMAServer::modify_mem_service(int idx){
	long long input[3];
	bool ret = false;
	int remote_size = sizeof(long long) * 3, local_size = sizeof(bool);
	int rc;
	int read_bytes = 0;
	int total_read_bytes = 0;
	while (!rc && total_read_bytes < remote_size) {
		read_bytes = read(res->sock[idx], reinterpret_cast<void*>(input), remote_size);
		if (read_bytes > 0)
			total_read_bytes += read_bytes;
		else
			rc = read_bytes;
	}
	{
		std::lock_guard<std::mutex> lk(*mtx);
		auto iter = mem_seg.find(std::make_pair(input[0], input[1]));
		if(iter == mem_seg.end()){
			ret = false;
			fprintf(stderr, "Memory node cannot find memory segment\n");
		}
		else if(input[2] == 0 && iter->second == 3){
			ret = true;
			mem_seg.erase(iter);
		}
		else if(input[2] == 2 && iter->second == 1
			|| input[2] == 3 && iter->second == 2){
			ret = true;
			iter->second = input[2];
		}
		else{
			ret = false;
			fprintf(stderr, "Unexpected memory segment state\n");
		}
	}
	rc = write(res->sock[idx], reinterpret_cast<void*>(&ret), local_size);
	if (rc < local_size)
		fprintf(stderr, "Failed writing data during sock_sync_data\n");
	else
		rc = 0;
}

}  // namespace ROCKSDB_NAMESPACE