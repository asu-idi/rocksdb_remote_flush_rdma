#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace shm_package {

class PackContext {
 public:
  std::vector<std::string> package;
  std::unordered_map<std::string, std::unordered_map<void*, size_t>> ptr_to_idx;
  std::unordered_map<size_t, void*> idx_to_ptr;
  size_t add_package(void* ptr, std::string type);
  void append_uint64(char* str, uint64_t num);
  uint64_t get_uint64(char* str, size_t& offset);
  void append_uint64(int idx, uint64_t num);
  uint64_t get_uint64(int idx, size_t& offset);
  void append_str(int idx, const std::string& str);
  std::string get_str(int idx, size_t& offset);
  void append_uint32(int idx, uint32_t num);
  uint32_t get_uint32(int idx, size_t& offset);
  void append_byte(int idx, uint8_t value);
  uint8_t get_byte(int idx, size_t& offset);
  size_t get_size();
  void compile_to_string(char* str);
  void parse_from_string(char* str);
};

}  // namespace shm_package