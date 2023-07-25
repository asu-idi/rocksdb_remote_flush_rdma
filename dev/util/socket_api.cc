#include "util/socket_api.hpp"

#include <mutex>

std::mutex socket_api_mtx = std::mutex();
ssize_t read_data(int fd, void* buf, ssize_t len) {
  std::lock_guard<std::mutex> lock(socket_api_mtx);
  char* buf_ = reinterpret_cast<char*>(buf);
  ssize_t total = 0;
  while (total < len) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(fd, &read_fds);
    int ret = select(fd + 1, &read_fds, nullptr, nullptr, nullptr);
    if (ret == -1) {
      LOG("read_data: select error");
      assert(false);
    } else if (ret == 0) {
      continue;
    }

    ssize_t n = read(fd, buf_ + total, len - total);
    if (n == -1) {
      LOG("read_data: read error");
      assert(false);
    } else if (n == 0) {
      break;
    }
    total += n;
  }
  assert(total == len);

  return total;
}

ssize_t write_data(int fd, const void* buf, ssize_t len) {
  std::lock_guard<std::mutex> lock(socket_api_mtx);
  const char* buf_ = reinterpret_cast<const char*>(buf);
  ssize_t total = 0;
  while (total < len) {
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(fd, &write_fds);
    int ret = select(fd + 1, nullptr, &write_fds, nullptr, nullptr);
    if (ret == -1) {
      LOG("write_data: select error");
      assert(false);
    } else if (ret == 0) {
      continue;
    }

    ssize_t n = write(fd, buf_ + total, len - total);
    if (n == -1) {
      LOG("write_data: write error");
      assert(false);
    }
    total += n;
  }
  assert(total == len);
  return total;
}