#pragma once
#include <cstdint>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "memory/trans_concurrent_arena.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {
struct RemoteMemTable {
  void* data{nullptr};
  size_t data_size{0};
  void* meta{nullptr};
  size_t meta_size{0};
  uint64_t id{0};
  MemTableRep* memtable{nullptr};
  // gc
  TransConcurrentArena* arena{nullptr};
  MemTable::KeyComparator* key_cmp{nullptr};
  SliceTransform* prefix_extractor{nullptr};
  static RemoteMemTable* rebuild_remote_memTable(void* data, size_t data_size,
                                                 void* meta, size_t meta_size);
};
class DBImpl;
class RemoteMemTablePool {
 public:
  Status rebuild_remote_memtable(void* data, size_t data_size, void* meta,
                                 size_t meta_size);

  inline Status delete_remote_memtable(uint64_t id) {
    Status s = Status::OK();
    if (id2ptr_.find(id) == id2ptr_.end()) {
      s = Status::NotFound("id not found");
    } else {
      std::lock_guard<std::mutex> lock(mtx_);
      RemoteMemTable* rmem = id2ptr_[id];
      delete rmem->memtable;
      delete rmem->arena;
      delete rmem->key_cmp;
      delete rmem->prefix_extractor;
      free(rmem->data);
      free(rmem->meta);
      id2ptr_.erase(id);
      delete rmem;
    }
    return s;
  }

  void store(void* data, int64_t data_size, void* meta, int64_t meta_size) {
    void* cp_data = malloc(data_size);
    memcpy(cp_data, data, data_size);
    void* cp_meta = malloc(meta_size);
    memcpy(cp_meta, meta, meta_size);
    std::thread{[this, cp_data, data_size, cp_meta, meta_size]() {
      Status s =
          rebuild_remote_memtable(cp_data, data_size, cp_meta, meta_size);
      if (!s.ok()) {
        fprintf(stderr, "rebuild_remote_memtable failed\n");
      }
    }}.detach();
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