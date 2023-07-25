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
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/logger.hpp"
#include "util/socket_api.hpp"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
class ReadOnlySkipListRep : public MemTableRep {
 public:
  static void PackLocal(
      int sockfd,
      const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
          readonly_skiplistrep) {
    int64_t msg = 0;
    readonly_skiplistrep->PackLocal(sockfd);
    auto readonly_skiplistrep_ = new ReadOnlySkipListRep(readonly_skiplistrep);
    LOG("ReadOnlySkipListRep::PackLocal: after "
        "ReadOnlyInlineSkiplist::PackLocal, start send len: ",
        sizeof(ReadOnlySkipListRep));
    write_data(sockfd, reinterpret_cast<void*>(readonly_skiplistrep_),
               sizeof(ReadOnlySkipListRep));
    LOG("ReadOnlySkipListRep::PackLocal: after send len: ",
        sizeof(ReadOnlySkipListRep));
    int64_t ret_val = 0;
    read_data(sockfd, &ret_val, sizeof(ret_val));
    LOG("ReadOnlySkipListRep::PackLocal: after read ret_val:", ret_val);
  }

  static void* UnPackLocal(int sockfd) {
    int64_t msg = 0;
    void* readonly_inline_skiplistrep =
        ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>::UnPackLocal(
            sockfd);
    auto* local_readonly_skiplistrep = new ReadOnlySkipListRep(nullptr);
    void* mem = reinterpret_cast<void*>(local_readonly_skiplistrep);
    void* mem2 = malloc(sizeof(ReadOnlySkipListRep));
    LOG("ReadOnlySkipListRep::UnPackLocal: after UnPackLocal ReadOnlyInlineSki"
        "plist, start read len: ",
        sizeof(ReadOnlySkipListRep));
    read_data(sockfd, mem2, sizeof(ReadOnlySkipListRep));
    LOG("ReadOnlySkipListRep::UnPackLocal: after read len: ",
        sizeof(ReadOnlySkipListRep));
    local_readonly_skiplistrep->read_only_skip_list_ = reinterpret_cast<
        const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*>(
        readonly_inline_skiplistrep);
    int64_t ret_val = 0;
    write_data(sockfd, &ret_val, sizeof(ret_val));
    LOG("ReadOnlySkipListRep::UnPackLocal: after send ret_val:", ret_val);
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

  KeyHandle Allocate(const size_t len, char** buf) override { assert(false); }

  void Insert(KeyHandle handle) override { assert(false); }
  bool InsertKey(KeyHandle) override { assert(false); }
  void InsertWithHint(KeyHandle, void**) override { assert(false); }
  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    assert(false);
  }
  void InsertConcurrently(KeyHandle handle) override { assert(false); }
  bool InsertKeyConcurrently(KeyHandle handle) override { assert(false); }
  bool Contains(const char* key) const override { assert(false); }
  size_t ApproximateMemoryUsage() override { assert(false); }
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
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
    void Seek(const Slice& user_key, const char* memtable_key) override {
      assert(false);
    }

    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      assert(false);
    }
    void RandomSeek() override { assert(false); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { assert(false); }
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    void* mem = new ReadOnlySkipListRep::Iterator(read_only_skip_list_);
    return new (mem) ReadOnlySkipListRep::Iterator(read_only_skip_list_);
  }
};

}  // namespace ROCKSDB_NAMESPACE