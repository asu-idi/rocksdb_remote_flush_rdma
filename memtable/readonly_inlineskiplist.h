#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <thread>
#include <type_traits>

#include "db/lookup_key.h"
#include "db/memtable.h"
#include "memory/allocator.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/iterator.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
class ReadOnlySkipListRep : public MemTableRep {
 public:
  using MemTableRep::PackLocal;
  static void PackLocal(
      TransferService* node,
      const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
          readonly_skiplistrep) {
    readonly_skiplistrep->PackLocal(node);
    auto readonly_skiplistrep_ = new ReadOnlySkipListRep(readonly_skiplistrep);
    node->send(reinterpret_cast<void*>(readonly_skiplistrep_),
               sizeof(ReadOnlySkipListRep));
    delete readonly_skiplistrep_;
  }

  static void* UnPackLocal(TransferService* node) {
    void* readonly_inline_skiplistrep =
        ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>::UnPackLocal(
            node);
    auto* local_readonly_skiplistrep = new ReadOnlySkipListRep(nullptr);
    void* mem = reinterpret_cast<void*>(local_readonly_skiplistrep);
    void* mem2 = malloc(sizeof(ReadOnlySkipListRep));
    LOG("ReadOnlySkipListRep::UnPackLocal: after UnPackLocal ReadOnlyInlineSki"
        "plist, start read len: ",
        sizeof(ReadOnlySkipListRep));
    size_t size = sizeof(ReadOnlySkipListRep);
    node->receive(&mem2, &size);
    LOG("ReadOnlySkipListRep::UnPackLocal: after read len: ",
        sizeof(ReadOnlySkipListRep));
    local_readonly_skiplistrep->read_only_skip_list_ = reinterpret_cast<
        const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*>(
        readonly_inline_skiplistrep);
    LOG("ReadOnlySkipListRep::UnPackLocal");
    free(mem2);
    return mem;
  }

 private:
  const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
      read_only_skip_list_;

  friend class LookaheadIterator;

 public:
  explicit ReadOnlySkipListRep(
      const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
          readonly_skiplistrep)
      : MemTableRep(nullptr), read_only_skip_list_(readonly_skiplistrep) {}

  KeyHandle Allocate(const size_t, char**) override {
    assert(false);
    return KeyHandle{};
  }

  void Insert(KeyHandle) override { assert(false); }
  bool InsertKey(KeyHandle) override {
    assert(false);
    return false;
  }
  void InsertWithHint(KeyHandle, void**) override { assert(false); }
  void InsertWithHintConcurrently(KeyHandle, void**) override { assert(false); }
  void InsertConcurrently(KeyHandle) override { assert(false); }
  bool InsertKeyConcurrently(KeyHandle) override {
    assert(false);
    return false;
  }
  bool Contains(const char*) const override {
    assert(false);
    return false;
  }
  size_t ApproximateMemoryUsage() override {
    assert(false);
    return 0;
  }
  void Get(const LookupKey&, void*,
           bool (*)(void* arg, const char* entry)) override {
    assert(false);
  }
  ~ReadOnlySkipListRep() override { delete read_only_skip_list_; }
  class Iterator : public MemTableRep::Iterator {
    ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    explicit Iterator(
        const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}
    ~Iterator() override = default;

    bool Valid() const override { return iter_.Valid(); }
    const char* key() const override { return iter_.key(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    void Seek(const Slice&, const char*) override { assert(false); }

    void SeekForPrev(const Slice&, const char*) override { assert(false); }
    void RandomSeek() override { assert(false); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { assert(false); }
  };

  MemTableRep::Iterator* GetIterator(Arena* = nullptr) override {
    void* mem = new ReadOnlySkipListRep::Iterator(read_only_skip_list_);
    return new (mem) ReadOnlySkipListRep::Iterator(read_only_skip_list_);
  }
};

}  // namespace ROCKSDB_NAMESPACE