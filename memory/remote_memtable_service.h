#pragma once
#include <cstdint>
#include <unordered_map>

#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
namespace ROCKSDB_NAMESPACE {
class RemoteMemTablePool {
  struct RemoteMemTable {
    void* data{nullptr};
    size_t data_size{0};
    void* meta{nullptr};
    size_t meta_size{0};
    uint64_t id{0};
    MemTableRep* memtable{nullptr};
  };

  Status rebuild_remote_memtable(void* data, size_t data_size, void* meta,
                                 size_t meta_size);

 public:
  void store(void* data, size_t data_size, void* meta, size_t meta_size) {
    void* cp_data = malloc(data_size);
    memcpy(cp_data, data, data_size);
    void* cp_meta = malloc(meta_size);
    memcpy(cp_meta, meta, meta_size);

    Status s = rebuild_remote_memtable(cp_data, data_size, cp_meta, meta_size);
    if (!s.ok()) {
      fprintf(stderr, "rebuild_remote_memtable failed\n");
    }
  }
  RemoteMemTable* get(uint64_t id) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = id2ptr_.find(id);
    if (it == id2ptr_.end()) {
      return nullptr;
    }
    return it->second;
  }

  void erase(uint64_t id) {
    std::lock_guard<std::mutex> lock(mtx_);
    id2ptr_.erase(id);
  }

 private:
  std::mutex mtx_;
  std::unordered_map<uint64_t, RemoteMemTable*> id2ptr_;
};
}  // namespace ROCKSDB_NAMESPACE