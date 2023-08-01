#include "memory/shared_package.h"

#include <cstring>
namespace shm_package {

size_t PackContext::add_package(void* ptr, std::string type) {
  size_t idx = package.size();
  ptr_to_idx[type][ptr] = idx;
  package.push_back(std::string());
  return idx;
}

void PackContext::append_uint64(char* str, uint64_t num) {
  *(str++) = num >> 56 & 0xffu;
  *(str++) = num >> 48 & 0xffu;
  *(str++) = num >> 40 & 0xffu;
  *(str++) = num >> 32 & 0xffu;
  *(str++) = num >> 24 & 0xffu;
  *(str++) = num >> 16 & 0xffu;
  *(str++) = num >> 8 & 0xffu;
  *(str++) = num & 0xffu;
}
void PackContext::append_uint64(int idx, uint64_t num) {
  package[idx].push_back(num >> 56 & 0xffu);
  package[idx].push_back(num >> 48 & 0xffu);
  package[idx].push_back(num >> 40 & 0xffu);
  package[idx].push_back(num >> 32 & 0xffu);
  package[idx].push_back(num >> 24 & 0xffu);
  package[idx].push_back(num >> 16 & 0xffu);
  package[idx].push_back(num >> 8 & 0xffu);
  package[idx].push_back(num & 0xffu);
}
uint64_t PackContext::get_uint64(char* str, size_t& offset) {
  uint64_t res = 0;
  for (int i = 0; i < 8; i++) res = res << 8 | str[offset++];
  return res;
}
uint64_t PackContext::get_uint64(int idx, size_t& offset) {
  uint64_t res = 0;
  for (int i = 0; i < 8; i++) res = res << 8 | package[idx][offset++];
  return res;
}
void PackContext::append_str(int idx, const std::string& str) {
  append_uint64(idx, str.size());
  package[idx].append(str);
}
std::string PackContext::get_str(int idx, size_t& offset) {
  size_t size = get_uint64(idx, offset);
  offset += size;
  return std::string(package[idx], offset - size, size);
}
void PackContext::append_uint32(int idx, uint32_t num) {
  package[idx].push_back(num >> 24 & 0xffu);
  package[idx].push_back(num >> 16 & 0xffu);
  package[idx].push_back(num >> 8 & 0xffu);
  package[idx].push_back(num & 0xffu);
}
uint32_t PackContext::get_uint32(int idx, size_t& offset) {
  uint32_t res = 0;
  for (int i = 0; i < 4; i++) res = res << 8 | package[idx][offset++];
  return res;
}
void PackContext::append_byte(int idx, uint8_t num) {
  package[idx].push_back(num);
}
uint8_t PackContext::get_byte(int idx, size_t& offset) {
  return (uint8_t)package[idx][offset++];
}

size_t PackContext::get_size() {
  size_t size = (1 + package.size()) * sizeof(uint64_t);
  for (size_t i = 0; i < package.size(); i++) size += package[i].size();
  return size;
}
void PackContext::compile_to_string(char* str) {
  append_uint64(str, package.size()), str += sizeof(uint64_t);
  for (size_t i = 0; i < package.size(); i++) {
    append_uint64(str, package[i].size()), str += sizeof(uint64_t);
    memcpy(str, package[i].c_str(), package[i].size()),
        str += package[i].size();
  }
}
void PackContext::parse_from_string(char* str) {
  package.clear();
  ptr_to_idx.clear();

  size_t offset = 0;
  for (int i = 0, tot = get_uint64(str, offset); i < tot; i++) {
    size_t size = get_uint64(str, offset);
    package.emplace_back(str + offset, size);
    offset += size;
  }
}

}  // namespace shm_package
