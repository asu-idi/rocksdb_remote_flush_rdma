#pragma once

#include "memory/remote_flush_service.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {

class TransferService {
 public:
  virtual ~TransferService() = default;
  virtual bool send(const void *buf, size_t size) = 0;
  virtual bool receive(void *buf, size_t size) = 0;
  virtual bool receive(void **buf, size_t *size) = 0;
  virtual bool receive(void **buf, size_t size) = 0;
};

class TCPTransferService : public TransferService {
 public:
  explicit TCPTransferService(TCPNode *service_provider)
      : service_provider_(service_provider) {
    assert(service_provider_ != nullptr);
  }
  ~TCPTransferService() = default;
  bool send(const void *buf, size_t size) override {
    return service_provider_->send(buf, size);
  }
  bool receive(void *buf, size_t size) override {
    return service_provider_->receive(buf, size);
  }
  bool receive(void **buf, size_t *size) override {
    return service_provider_->receive(buf, size);
  }
  bool receive(void **buf, size_t size) override {
    return service_provider_->receive(buf, size);
  }

 private:
  TCPNode *service_provider_;
};

class RDMATransferService : public TransferService {
 public:
  explicit RDMATransferService(RDMAClient *service_provider)
      : service_provider_(service_provider) {
    assert(service_provider_ != nullptr);
    assert(service_provider->get_buf() != nullptr);
    current_ptr = service_provider_->get_buf();
    offset_ = 0;
  }
  explicit RDMATransferService(RDMAClient *service_provider, size_t offset)
      : service_provider_(service_provider) {
    assert(service_provider_ != nullptr);
    assert(service_provider->get_buf() != nullptr);
    current_ptr = service_provider_->get_buf() + offset;
    offset_ = offset;
  }

  bool send(const void *buf, size_t size) override {
    const char *hheader = "Header";
    memcpy(current_ptr, hheader, 6);
    memcpy(current_ptr + 6, &size, sizeof(size_t));
    current_ptr += sizeof(size_t) + 6;
    memcpy(current_ptr, buf, size);
    current_ptr += size;
    return true;
  }
  ~RDMATransferService() = default;
  bool receive(void *buf, size_t size) override {
    assert(buf != nullptr);
    return receive(&buf, size);
  }
  bool receive(void **buf, size_t size) override {
    assert(size != 0);
    return receive(buf, &size);
  }
  bool receive(void **buf, size_t *size) override {
    char *buf_ = reinterpret_cast<char *>(*buf);
    assert(memcmp(current_ptr, "Header", 6) == 0);
    size_t package_size =
        *reinterpret_cast<size_t *>(reinterpret_cast<char *>(current_ptr) + 6);
    current_ptr += sizeof(size_t) + 6;
    if (*size == 0)
      *size = package_size;
    else
      assert(package_size == *size);

    if (buf_ == nullptr) {
      buf_ = reinterpret_cast<char *>(
          service_provider_->memory_.allocate(package_size));
      *buf = reinterpret_cast<void *>(buf_);
    }
    LOG("RDMANode read from buffer");
    memcpy(buf_, current_ptr, package_size);
    current_ptr += package_size;
    return true;
  }
  size_t get_size() { return current_ptr - (service_provider_->get_buf() + offset_);}

 private:
  RDMAClient *service_provider_;
  char *current_ptr = nullptr;
  size_t offset_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE