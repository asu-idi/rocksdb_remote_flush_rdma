#include "memory/remote_memtable_service.h"

#include <cstdint>
#include <mutex>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/concurrent_arena.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {
RemoteMemTable* RemoteMemTable::rebuild_remote_memTable(void* data,
                                                        size_t data_size,
                                                        void* meta,
                                                        size_t meta_size) {
  auto* rmt = new RemoteMemTable();
  uint64_t id_ = 0;
  int32_t head_offset_ = 0;
  bool cmp_id = false;
  std::pair<int64_t, int64_t> transform_id = {0, 0};
  size_t lookahead_ = 0;
  void* skip_list_ptr_ = nullptr;
  void* begin_ptr_ = nullptr;

  //   parse meta
  char* meta_ptr = reinterpret_cast<char*>(meta);
  id_ = *reinterpret_cast<uint64_t*>(meta_ptr);
  meta_ptr += sizeof(uint64_t);
  head_offset_ = *reinterpret_cast<int32_t*>(meta_ptr);
  meta_ptr += sizeof(int32_t);
  cmp_id = *reinterpret_cast<bool*>(meta_ptr);
  meta_ptr += sizeof(bool);
  transform_id.first = *reinterpret_cast<int64_t*>(meta_ptr);
  meta_ptr += sizeof(int64_t);
  if (transform_id.first == 0) {
    transform_id.second = *reinterpret_cast<int64_t*>(meta_ptr);
    meta_ptr += sizeof(int64_t);
  }
  lookahead_ = *reinterpret_cast<size_t*>(meta_ptr);
  meta_ptr += sizeof(size_t);
  skip_list_ptr_ = *reinterpret_cast<void**>(meta_ptr);
  meta_ptr += sizeof(void*);
  begin_ptr_ = *reinterpret_cast<void**>(meta_ptr);
  meta_ptr += sizeof(void*);

  // rebuild
  rmt->data = data;
  rmt->data_size = data_size;
  rmt->meta = meta;
  rmt->meta_size = meta_size;
  rmt->id = id_;

  LOG_CERR("rebuild rmem id:", id_, ' ', "head_offset:", head_offset_, ' ',
           "cmp_id:", cmp_id, ' ', "transform_id:", transform_id.first, ' ',
           "lookahead:", lookahead_, ' ', "skip_list_ptr:", skip_list_ptr_, ' ',
           "begin_ptr:", begin_ptr_, ' ', "data_size:", data_size, ' ',
           "meta_size:", meta_size, ' ', "data:", data, ' ', "meta:", meta);

  auto* key_cmp = new MemTable::KeyComparator(
      (!cmp_id) ? (InternalKeyComparator(BytewiseComparator()))
                : InternalKeyComparator(ReverseBytewiseComparator()));
  rmt->key_cmp = key_cmp;
  auto* arena = new TransConcurrentArena(1024);
  rmt->arena = arena;
  const SliceTransform* prefix_extractor = nullptr;
  if (transform_id.first == 0xff) {
    prefix_extractor = nullptr;
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 1)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewFixedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 2)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewCappedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0 && ((transform_id.second & 0xff) == 3)) {
    prefix_extractor = new InternalKeySliceTransform(NewNoopTransform());
  } else if ((transform_id.first & 0xff) == 1) {
    prefix_extractor = NewFixedPrefixTransform(transform_id.first >> 8);
  } else if ((transform_id.first & 0xff) == 2) {
    prefix_extractor = NewCappedPrefixTransform(transform_id.first >> 8);
  } else if ((transform_id.first & 0xff) == 3) {
    prefix_extractor = NewNoopTransform();
  } else {
    assert(false);
    prefix_extractor = nullptr;
  }
  rmt->prefix_extractor = const_cast<SliceTransform*>(prefix_extractor);

  auto* skip_list_factory = new SkipListFactory(lookahead_);
  MemTableRep* rmt_rep = skip_list_factory->CreateMemTableRep(
      *key_cmp, arena, prefix_extractor, nullptr);
  delete skip_list_factory;
  //   rmt_rep->MemnodeRebuild(skip_list_ptr_);
  rmt_rep->set_local_begin(data);
  rmt_rep->set_head_offset(head_offset_);
  rmt->memtable = rmt_rep;
  return rmt;
}

Status RemoteMemTablePool::rebuild_remote_memtable(void* data, size_t data_size,
                                                   void* meta,
                                                   size_t meta_size) {
  Status s = Status::OK();
  uint64_t id_ = *reinterpret_cast<uint64_t*>(meta);
  {
    std::lock_guard<std::mutex> lck(mtx_);
    if (id2ptr_.find(id_) != id2ptr_.end()) {
      fprintf(stderr, "rebuild_remote_memtable id %lu already exists\n", id_);
      free(data);
      free(meta);
      return Status::OK();
    }
  }
  RemoteMemTable* rmt =
      RemoteMemTable::rebuild_remote_memTable(data, data_size, meta, meta_size);
  if (rmt == nullptr) {
    s = Status::NotFound("rebuild parse failed");
    free(data);
    free(meta);
    return s;
  }
  if (s.ok()) {
    std::lock_guard<std::mutex> lck(mtx_);
    if (id2ptr_.find(id_) != id2ptr_.end()) {
      fprintf(stderr, "rebuild_remote_memtable id %lu already exists\n", id_);
      delete rmt->memtable;
      delete rmt->arena;
      delete rmt->key_cmp;
      delete rmt->prefix_extractor;
      free(rmt->data);
      free(rmt->meta);
      delete rmt;
      return Status::OK();
    } else {
      id2ptr_[id_] = rmt;
    }
  }
  fprintf(stderr, "rebuild_remote_memtable done\n");
  return s;
}
}  // namespace ROCKSDB_NAMESPACE