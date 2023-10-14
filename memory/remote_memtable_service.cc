#include "memory/remote_memtable_service.h"

#include <cstdint>
#include <mutex>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/concurrent_arena.h"
#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {
Status RemoteMemTablePool::rebuild_remote_memtable(void* data, size_t data_size,
                                                   void* meta,
                                                   size_t meta_size) {
  Status s = Status::OK();

  auto* rmt = new RemoteMemTable();
  uint64_t id_ = 0;
  int32_t type = 0;
  bool cmp_id = false;
  std::pair<int64_t, int64_t> transform_id = {0, 0};
  size_t lookahead_ = 0;
  void* skip_list_ptr_ = nullptr;
  void* begin_ptr_ = nullptr;

  //   parse meta
  char* meta_ptr = reinterpret_cast<char*>(meta);
  id_ = *reinterpret_cast<uint64_t*>(meta_ptr);
  meta_ptr += meta_size;
  type = *reinterpret_cast<int32_t*>(meta_ptr);
  meta_ptr += sizeof(int32_t);
  cmp_id = *reinterpret_cast<bool*>(meta_ptr);
  meta_ptr += sizeof(bool);
  transform_id.first = *reinterpret_cast<int64_t*>(meta_ptr);
  meta_ptr += sizeof(int64_t);
  if (transform_id.first == 0xff) {
    transform_id.second = *reinterpret_cast<int64_t*>(meta_ptr);
    meta_ptr += sizeof(int64_t);
  }
  lookahead_ = *reinterpret_cast<size_t*>(meta_ptr);
  meta_ptr += sizeof(size_t);
  skip_list_ptr_ = reinterpret_cast<void*>(meta_ptr);
  meta_ptr += sizeof(void*);
  begin_ptr_ = reinterpret_cast<void*>(meta_ptr);
  meta_ptr += sizeof(void*);

  // rebuild
  rmt->data = data;
  rmt->data_size = data_size;
  rmt->meta = meta;
  rmt->meta_size = meta_size;
  rmt->id = id_;

  auto* key_cmp = new MemTable::KeyComparator(
      (!cmp_id) ? (InternalKeyComparator(BytewiseComparator()))
                : InternalKeyComparator(BytewiseComparator()));
  auto* arena = new ConcurrentArena;
  const SliceTransform* prefix_extractor = nullptr;
  if (transform_id.first == 0xff && ((transform_id.second & 0xff) == 1)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewFixedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0xff &&
             ((transform_id.second & 0xff) == 1)) {
    prefix_extractor = new InternalKeySliceTransform(
        NewCappedPrefixTransform(transform_id.second >> 8));
  } else if (transform_id.first == 0xff &&
             ((transform_id.second & 0xff) == 2)) {
    prefix_extractor = new InternalKeySliceTransform(NewNoopTransform());
  } else if ((transform_id.second & 0xff) == 1) {
    prefix_extractor = NewFixedPrefixTransform(transform_id.second >> 8);
  } else if ((transform_id.second & 0xff) == 2) {
    prefix_extractor = NewCappedPrefixTransform(transform_id.second >> 8);
  } else if ((transform_id.second & 0xff) == 3) {
    prefix_extractor = NewNoopTransform();
  } else {
    s = Status::NotSupported("transform_id.second is not supported");
  }

  auto* skip_list_factory = new SkipListFactory(lookahead_);
  MemTableRep* rmt_rep = skip_list_factory->CreateMemTableRep(
      *key_cmp, arena, prefix_extractor, nullptr);
  delete skip_list_factory;
  //   rmt_rep->MemnodeRebuild(skip_list_ptr_);
  rmt_rep->set_local_begin(data);
  rmt->memtable = rmt_rep;
  if (s.ok()) {
    std::lock_guard<std::mutex> lck(mtx_);
    id2ptr_[id_] = rmt;
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE