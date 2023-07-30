
#pragma once

#include <stdint.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstdarg>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "env/io_posix.h"
#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/thread_status.h"
#include "util/socket_api.hpp"

namespace ROCKSDB_NAMESPACE {

class FSDirectoryFactory {
 public:
  static void* UnPackLocal(int sockfd);

 public:
  FSDirectoryFactory& operator=(const FSDirectoryFactory&) = delete;
  FSDirectoryFactory(FSDirectoryFactory&&) = delete;
  FSDirectoryFactory& operator=(FSDirectoryFactory&&) = delete;
  FSDirectoryFactory(const FSDirectoryFactory&) = delete;

 private:
  ~FSDirectoryFactory() = default;
  FSDirectoryFactory() = default;
};

class DirectoryPackFactory {
 public:
  static void* UnPackLocal(int sockfd);

 public:
  DirectoryPackFactory(const DirectoryPackFactory&) = delete;
  DirectoryPackFactory(DirectoryPackFactory&&) = delete;
  DirectoryPackFactory& operator=(const DirectoryPackFactory&) = delete;
  DirectoryPackFactory& operator=(DirectoryPackFactory&&) = delete;

 private:
  DirectoryPackFactory() = default;
  ~DirectoryPackFactory() = default;
};

inline void* DirectoryPackFactory::UnPackLocal(int sockfd) {
  LOG("DirectoryPackFactory::UnPackLocal");
  size_t msg = 0;
  read_data(sockfd, &msg, sizeof(msg));
  send(sockfd, &msg, sizeof(msg), 0);
  size_t type = msg & 0xff;
  size_t info = msg >> 8;
  LOG("DirectoryPackFactory::UnPackLocal: type:", type, " info:", info);
  assert(type == 2);
  int fd = 0;
  read_data(sockfd, &fd, sizeof(fd));
  write(sockfd, &msg, sizeof(msg));
  size_t directory_name_size_ = 0;
  read_data(sockfd, &directory_name_size_, sizeof(directory_name_size_));
  write(sockfd, &msg, sizeof(msg));
  std::string directory_name_;
  directory_name_.resize(directory_name_size_);
  read_data(sockfd, directory_name_.data(), directory_name_size_);
  write(sockfd, &msg, sizeof(msg));
  auto* ptr = new PosixDirectory(fd, directory_name_);
  return reinterpret_cast<void*>(ptr);
}

inline void* FSDirectoryFactory::UnPackLocal(int sockfd) {
  size_t msg = 0;
  read_data(sockfd, &msg, sizeof(msg));
  send(sockfd, &msg, sizeof(msg), 0);
  size_t type = msg & 0xff;
  size_t info = msg >> 8;
  if (type == 0) {
    LOG("FSDirectoryFactory::UnPackLocal: nullptr");
    return nullptr;
  } else if (type == 1 /*FSDirectoryWrapper*/) {
    LOG("FSDirectoryFactory::UnPackLocal: FSDirectoryWrapper recursion");
    void* ret = UnPackLocal(sockfd);
    auto* pptr = reinterpret_cast<FSDirectory*>(ret);
    auto* ptr = new FSDirectoryWrapper(std::unique_ptr<FSDirectory>(pptr));
    return reinterpret_cast<void*>(ptr);
  } else if (type == 2 /*PosixDirectory*/) {
    LOG("FSDirectoryFactory::UnPackLocal: PosixDirectory ");
    int fd = 0;
    read_data(sockfd, &fd, sizeof(fd));
    write(sockfd, &msg, sizeof(msg));
    // bool is_btrfs_ = false;
    // read_data(sockfd, &is_btrfs_, sizeof(is_btrfs_));
    // write(sockfd, &msg, sizeof(msg));
    size_t directory_name_size_ = 0;
    read_data(sockfd, &directory_name_size_, sizeof(directory_name_size_));
    write(sockfd, &msg, sizeof(msg));
    std::string directory_name_;
    directory_name_.resize(directory_name_size_);
    read_data(sockfd, directory_name_.data(), directory_name_size_);
    write(sockfd, &msg, sizeof(msg));
    auto* ptr = new PosixDirectory(fd, directory_name_);
    return reinterpret_cast<void*>(ptr);
  } else if (type == 3 /*LegacyDirectoryWrapper*/) {
    LOG("FSDirectoryFactory::UnPackLocal: LegacyDirectoryWrapper recursion");
    if (info) assert(false);
    void* directory = DirectoryPackFactory::UnPackLocal(sockfd);
    LOG("check local file system:", Env::Default()->GetFileSystem()->Name());
    return directory;
  } else if (type == 4 /*Mock*/) {
    return nullptr;
  } else {
    LOG("FSDirectoryFactory::UnPackLocal: unknown type:", type);
    assert(false);
  }
}

}  // namespace ROCKSDB_NAMESPACE