#pragma once

#include <asm-generic/errno-base.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#define ASSERT_RW(value) \
  do {                   \
    assert(value);       \
  } while (0)

inline ssize_t readn(int fd, void *vptr, size_t n) {
  ssize_t nread;
  char *ptr = (char *)vptr;
  size_t nleft = n;
  while (nleft > 0) {
    if ((nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR)
        nread = 0;
      else {
        fprintf(stderr, "read error, return %lu, nleft %lu\n", nread, nleft);
        return -1;
      }
    }
    nleft -= nread;
    ptr += nread;
  }
  return (ssize_t)(n - nleft);
}

inline ssize_t writen(int fd, const void *vptr, size_t n) {
  ssize_t nwritten;
  const char *ptr = (const char *)vptr;
  size_t nleft = n;
  while (nleft > 0) {
    if ((nwritten = write(fd, ptr, nleft)) < 0) {
      if (nwritten < 0 && errno == EINTR)
        nwritten = 0;
      else {
        fprintf(stderr, "writen error, return %lu, nleft %lu\n", nwritten,
                nleft);
        return -1;
      }
    }
    nleft -= nwritten;
    ptr += nwritten;
  }
  return (ssize_t)n;
}