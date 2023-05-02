#include <chrono>
#include <cstddef>
#include <random>
#include <thread>
#include <type_traits>

#include "db/lookup_key.h"
#include "db/memtable.h"
#include "memory/allocator.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/logger.hpp"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
class ReadOnlySkipListRep : public MemTableRep {
  const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
      read_only_skip_list_;
  const MemTableRep::KeyComparator& cmp_;
  const size_t lookahead_;

  friend class LookaheadIterator;

 public:
  explicit ReadOnlySkipListRep(
      const MemTableRep::KeyComparator& compare, Allocator* allocator,
      const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
          readonly_skiplistrep);
  KeyHandle Allocate(const size_t len, char** buf) override {
    LOG("[ERROR] readonlySkipListRep not support Allocate");
    assert(false);
  }

  void Insert(KeyHandle handle) override {
    LOG("[ERROR] readonlySkipListRep not support Insert");
    assert(false);
  }
  bool InsertKey(KeyHandle) override {
    LOG("[ERROR] readonlySkipListRep not support InsertKey");
    assert(false);
  }
  void InsertWithHint(KeyHandle, void**) override {
    LOG("[ERROR] readonlySkipListRep not support InsertWithHint");
    assert(false);
  }
  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    LOG("[ERROR] readonlySkipListRep not support InsertWithHintConcurrently");
    assert(false);
  }
  void InsertConcurrently(KeyHandle handle) override {
    LOG("[ERROR] readonlySkipListRep not support InsertConcurrently");
    assert(false);
  }
  bool InsertKeyConcurrently(KeyHandle handle) override {
    LOG("[ERROR] readonlySkipListRep not support InsertKeyConcurrently");
    assert(false);
  }
  bool Contains(const char* key) const override {
    LOG("[WARN] maybe no need to support Contain");
    std::this_thread::sleep_for(std::chrono::seconds(5));
    return false;
  }
  size_t ApproximateMemoryUsage() override { return 0; }
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;
  ~ReadOnlySkipListRep() override {}
  class Iterator : public MemTableRep::Iterator {
    ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    explicit Iterator(
        const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}
    ~Iterator() override {}

    bool Valid() const override { return iter_.Valid(); }
    const char* key() const override { return iter_.key(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        std::string tmp;
        iter_.Seek(EncodeKey(&tmp, memtable_key));
      }
    }

    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      LOG("[ERROR] not supported");
      assert(false);
    }
    void RandomSeek() override {
      LOG("[ERROR] not supported");
      assert(false);
    }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override {
      LOG("[ERROR] not supported");
      assert(false);
    }
  };
  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    assert(lookahead_ == 0);
    void* mem =
        arena ? arena->AllocateAligned(sizeof(ReadOnlySkipListRep::Iterator)) :
              operator new(sizeof(ReadOnlySkipListRep::Iterator));
    // note: allocate in local memory
    return new (mem) ReadOnlySkipListRep::Iterator(read_only_skip_list_);
  }
};

}  // namespace ROCKSDB_NAMESPACE