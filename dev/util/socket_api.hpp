#pragma once
#include <cassert>
#include <cstdio>
#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#else
#include <winsock2.h>
#endif  //__linux__
#include "util/logger.hpp"

ssize_t read_data(int fd, void* buf, ssize_t len);
ssize_t write_data(int fd, const void* buf, ssize_t len);