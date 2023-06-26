#include "memory/shared_package.h"
namespace shm_package {
char *Pack(const std::string &raw) {
  if (raw.empty()) return nullptr;
  char *ret = shm_alloc(raw.length());
  strncpy(ret, raw.data(), raw.length());
  return ret;
}

void Unpack(void *u, std::string &t, size_t length) {
  // assert(t.empty());
  t.resize(length);
  if (length) strncpy(t.data(), reinterpret_cast<char *>(u), length);
}

}  // namespace shm_package
