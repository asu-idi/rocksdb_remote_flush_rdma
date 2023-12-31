#pragma once

#include "rocksdb/logger.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/status.h"

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

class BufTransferService : public TransferService {
 public:
  explicit BufTransferService(void *buf, size_t size)
      : buf_(buf), size_(size), current_ptr_(static_cast<char *>(buf)) {}
  ~BufTransferService() = default;
  bool send_without_length(const void *buf, size_t size) {
    memcpy(current_ptr_, buf, size);
    current_ptr_ += size;
    return true;
  }
  bool send(const void *buf, size_t size) override {
    memcpy(current_ptr_, &size, sizeof(size_t));
    current_ptr_ += sizeof(size_t);
    memcpy(current_ptr_, buf, size);
    current_ptr_ += size;
    return true;
  }
  bool receive_without_len(void *buf, size_t size) {
    memcpy(buf, current_ptr_, size);
    current_ptr_ += size;
    return true;
  }
  bool receive(void *buf, size_t size) override { return receive(&buf, size); }
  bool receive(void **buf, size_t size) override { return receive(buf, &size); }
  bool receive(void **buf, size_t *size) override {
    char *buf_ = reinterpret_cast<char *>(*buf);
    size_t package_size = *reinterpret_cast<size_t *>(current_ptr_);
    current_ptr_ += sizeof(size_t);
    if (*size == 0)
      *size = package_size;
    else
      assert(package_size == *size);

    if (buf_ == nullptr) {
      buf_ = reinterpret_cast<char *>(memory_.allocate(package_size));
      *buf = reinterpret_cast<void *>(buf_);
    }
    memcpy(buf_, current_ptr_, package_size);
    current_ptr_ += package_size;
    return true;
  }
  size_t get_size() { return current_ptr_ - static_cast<char *>(buf_); }

 private:
  void *buf_;
  size_t size_;
  char *current_ptr_;
  RegularMemNode memory_;
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
    memcpy(current_ptr, &size, sizeof(size_t));
    current_ptr += sizeof(size_t);
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
    size_t package_size = *reinterpret_cast<size_t *>(current_ptr);
    current_ptr += sizeof(size_t);
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
  size_t get_size() {
    return current_ptr - (service_provider_->get_buf() + offset_);
  }

 private:
  RDMAClient *service_provider_;
  char *current_ptr = nullptr;
  size_t offset_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE