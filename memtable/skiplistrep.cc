//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/trans_concurrent_arena.h"
#include "memtable/inlineskiplist.h"
#include "memtable/readonly_inlineskiplist.h"
#include "memtable/trans_inlineskiplist.h"
#include "rocksdb/comparator.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/memtablerep.h"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class SkipListRep : public MemTableRep {
  friend SkipListFactory;
  friend ReadOnlySkipListRep;

 public:
  void TESTContinuous() const override {
    char* ptr = reinterpret_cast<char*>(const_cast<SkipListRep*>(this));
    LOG("SkipListRep: ", reinterpret_cast<void*>(ptr));
    ptr = reinterpret_cast<char*>(const_cast<size_t*>(&lookahead_));
    LOG("SkipListRep: lookahead: ", reinterpret_cast<void*>(ptr));
    ptr -= 8;
    LOG("SkipListRep: transform_: ", reinterpret_cast<void*>(ptr));
    ptr -= 8;
    LOG("SkipListRep: cmp_: ", reinterpret_cast<void*>(ptr));
    skip_list_.TESTContinuous();
    return;
  }
  Status SendToRemote(RDMAClient*, RDMANode::rdma_connection*,
                      const std::pair<size_t, size_t>&, size_t,
                      const std::pair<size_t, size_t>&, size_t, uint64_t,
                      int) override;
  void PackLocal(TransferService* node) const override;

 private:
  TransInlineSkipList<const MemTableRep::KeyComparator&> skip_list_;
  const MemTableRep::KeyComparator& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;
  std::atomic<bool> trans_finished_{false};

  friend class LookaheadIterator;

 public:
  explicit SkipListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, const SliceTransform* transform,
                       const size_t lookahead)
      : MemTableRep(allocator),
        skip_list_(compare, allocator),
        cmp_(compare),
        transform_(transform),
        lookahead_(lookahead) {
    LOG("CHECK SkiplistRep allocator:", std::hex, (long long)allocator,
        std::dec, allocator->name());
  }

  KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = skip_list_.AllocateKey(len);
    return static_cast<KeyHandle>(*buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    skip_list_.Insert(static_cast<char*>(handle));
  }

  bool InsertKey(KeyHandle handle) override {
    return skip_list_.Insert(static_cast<char*>(handle));
  }

  void InsertWithHint(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHint(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle),
                                                 hint);
  }

  void InsertConcurrently(KeyHandle handle) override {
    skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  // Returns true iff an entry that compares equal to key is in the list.
  // todo: remote support
  bool Contains(const char* key) const override {
    return skip_list_.Contains(key);
  }

  // todo: update this
  size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  // todo: remote support
  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    for (iter.Seek(dummy_slice, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }

  // todo: remote support (low pri)
  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    std::string tmp;
    uint64_t start_count =
        skip_list_.EstimateCount(EncodeKey(&tmp, start_ikey));
    uint64_t end_count = skip_list_.EstimateCount(EncodeKey(&tmp, end_ikey));
    return (end_count >= start_count) ? (end_count - start_count) : 0;
  }

  void UniqueRandomSample(const uint64_t num_entries,
                          const uint64_t target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    entries->clear();
    // Avoid divide-by-0.
    assert(target_sample_size > 0);
    assert(num_entries > 0);
    // NOTE: the size of entries is not enforced to be exactly
    // target_sample_size at the end of this function, it might be slightly
    // greater or smaller.
    SkipListRep::Iterator iter(&skip_list_);
    // There are two methods to create the subset of samples (size m)
    // from the table containing N elements:
    // 1-Iterate linearly through the N memtable entries. For each entry i,
    //   add it to the sample set with a probability
    //   (target_sample_size - entries.size() ) / (N-i).
    //
    // 2-Pick m random elements without repetition.
    // We pick Option 2 when m<sqrt(N) and
    // Option 1 when m > sqrt(N).
    if (target_sample_size >
        static_cast<uint64_t>(std::sqrt(1.0 * num_entries))) {
      Random* rnd = Random::GetTLSInstance();
      iter.SeekToFirst();
      uint64_t counter = 0, num_samples_left = target_sample_size;
      for (; iter.Valid() && (num_samples_left > 0); iter.Next(), counter++) {
        // Add entry to sample set with probability
        // num_samples_left/(num_entries - counter).
        if (rnd->Next() % (num_entries - counter) < num_samples_left) {
          entries->insert(iter.key());
          num_samples_left--;
        }
      }
    } else {
      // Option 2: pick m random elements with no duplicates.
      // If Option 2 is picked, then target_sample_size<sqrt(N)
      // Using a set spares the need to check for duplicates.
      for (uint64_t i = 0; i < target_sample_size; i++) {
        // We give it 5 attempts to find a non-duplicate
        // With 5 attempts, the chances of returning `entries` set
        // of size target_sample_size is:
        // PROD_{i=1}^{target_sample_size-1} [1-(i/N)^5]
        // which is monotonically increasing with N in the worse case
        // of target_sample_size=sqrt(N), and is always >99.9% for N>4.
        // At worst, for the final pick , when m=sqrt(N) there is
        // a probability of p= 1/sqrt(N) chances to find a duplicate.
        for (uint64_t j = 0; j < 5; j++) {
          iter.RandomSeek();
          // unordered_set::insert returns pair<iterator, bool>.
          // The second element is true if an insert successfully happened.
          // If element is already in the set, this bool will be false, and
          // true otherwise.
          if ((entries->insert(iter.key())).second) {
            break;
          }
        }
      }
    }
  }

  void MarkReadOnly() override;
  void MarkTransAsFinished() { trans_finished_.store(true); }

  ~SkipListRep() override {}

  // if we really have to support this (local flush fallback), load the table
  // back here.
  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    TransInlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
        const TransInlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}

    ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override { iter_.Prev(); }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.SeekForPrev(memtable_key);
      } else {
        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    void RandomSeek() override { iter_.RandomSeek(); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  class LookaheadIterator : public MemTableRep::Iterator {
   public:
    explicit LookaheadIterator(const SkipListRep& rep)
        : rep_(rep), iter_(&rep_.skip_list_), prev_(iter_) {}

    ~LookaheadIterator() override {}

    bool Valid() const override { return iter_.Valid(); }

    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    void Next() override {
      assert(Valid());

      bool advance_prev = true;
      if (prev_.Valid()) {
        auto k1 = rep_.UserKey(prev_.key());
        auto k2 = rep_.UserKey(iter_.key());

        if (k1.compare(k2) == 0) {
          // same user key, don't move prev_
          advance_prev = false;
        } else if (rep_.transform_) {
          // only advance prev_ if it has the same prefix as iter_
          auto t1 = rep_.transform_->Transform(k1);
          auto t2 = rep_.transform_->Transform(k2);
          advance_prev = t1.compare(t2) == 0;
        }
      }

      if (advance_prev) {
        prev_ = iter_;
      }
      iter_.Next();
    }

    void Prev() override {
      assert(Valid());
      iter_.Prev();
      prev_ = iter_;
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);

      if (prev_.Valid() && rep_.cmp_(encoded_key, prev_.key()) >= 0) {
        // prev_.key() is smaller or equal to our target key; do a quick
        // linear search (at most lookahead_ steps) starting from prev_
        iter_ = prev_;

        size_t cur = 0;
        while (cur++ <= rep_.lookahead_ && iter_.Valid()) {
          if (rep_.cmp_(encoded_key, iter_.key()) <= 0) {
            return;
          }
          Next();
        }
      }

      iter_.Seek(encoded_key);
      prev_ = iter_;
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
      prev_ = iter_;
    }

    void SeekToFirst() override {
      iter_.SeekToFirst();
      prev_ = iter_;
    }

    void SeekToLast() override {
      iter_.SeekToLast();
      prev_ = iter_;
    }

   protected:
    std::string tmp_;  // For passing to EncodeKey

   private:
    const SkipListRep& rep_;
    TransInlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;
    TransInlineSkipList<const MemTableRep::KeyComparator&>::Iterator prev_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (lookahead_ > 0) {
      void* mem =
          arena ? arena->AllocateAligned(sizeof(SkipListRep::LookaheadIterator))
                :
                operator new(sizeof(SkipListRep::LookaheadIterator));
      return new (mem) SkipListRep::LookaheadIterator(*this);
    } else {
      void* mem = arena ? arena->AllocateAligned(sizeof(SkipListRep::Iterator))
                        :
                        operator new(sizeof(SkipListRep::Iterator));
      return new (mem) SkipListRep::Iterator(&skip_list_);
    }
  }
};

Status SkipListRep::SendToRemote(
    RDMAClient* client, RDMANode::rdma_connection* conn,
    const std::pair<size_t, size_t>& remote_meta_seg, size_t local_meta_offset,
    const std::pair<size_t, size_t>& remote_data_seg, size_t local_data_offset,
    uint64_t memtable_id, int type) {
  Status s = Status::OK();
  void* begin = reinterpret_cast<TransConcurrentArena*>(allocator_)->begin();
  // send meta
  char* metadata_ = new char[53]();
  char* ptr = metadata_;
  *reinterpret_cast<uint64_t*>(ptr) = memtable_id;
  ptr += sizeof(uint64_t);
  *reinterpret_cast<int32_t*>(ptr) = type;
  ptr += sizeof(int32_t);

  // comparator
  auto cmp_id = reinterpret_cast<const MemTable::KeyComparator*>(&cmp_)
                    ->comparator.user_comparator()
                    ->Name();
  if (strcmp(cmp_id, BytewiseComparator()->Name()) == 0) {
    *reinterpret_cast<bool*>(ptr) = false;
  } else if (strcmp(cmp_id, ReverseBytewiseComparator()->Name()) == 0) {
    *reinterpret_cast<bool*>(ptr) = true;
  } else {
    fprintf(stderr, "cmp_ type not supported");
    delete[] metadata_;
    return Status::NotSupported("cmp_ type not supported");
  }
  // slicetransform
  std::function<bool(SliceTransform*&, char*&)> parser =
      [](SliceTransform*& now, char*& offset) -> bool {
    if (now == nullptr) {
      *reinterpret_cast<int64_t*>(offset) = 0xff;
      offset += sizeof(int64_t);
      return true;
    } else if (now->identifier() == 0) {
      *reinterpret_cast<int64_t*>(offset) = 0;
      offset += sizeof(int64_t);
      now = const_cast<SliceTransform*>(
          static_cast<InternalKeySliceTransform*>(now)
              ->user_prefix_extractor());
      return false;
    } else {
      *reinterpret_cast<int64_t*>(offset) = now->identifier();
      offset += sizeof(int64_t);
      return true;
    }
  };
  auto* trans_ = const_cast<SliceTransform*>(transform_);
  while (parser(trans_, ptr))
    ;

  *reinterpret_cast<size_t*>(ptr) = lookahead_;
  ptr += sizeof(size_t);

  *reinterpret_cast<void**>(ptr) = reinterpret_cast<void*>(&skip_list_);
  ptr += sizeof(void*);
  *reinterpret_cast<void**>(ptr) = begin;
  ptr += sizeof(void*);
  // 8+8+8+16+1+4+8 = 53
  std::memcpy(client->get_buf() + local_meta_offset, metadata_, 53);
  client->rdma_write(conn, 53, local_meta_offset, remote_meta_seg.first);
  if (client->poll_completion(conn)) {
    s = Status::IOError("poll_completion failed");
  }
  if (s.ok()) {
    std::memcpy(client->get_buf() + local_data_offset, begin,
                reinterpret_cast<TransConcurrentArena*>(allocator_)
                    ->get_max_allocated_bytes());
    client->rdma_write(conn,
                       reinterpret_cast<TransConcurrentArena*>(allocator_)
                           ->get_max_allocated_bytes(),
                       local_data_offset, remote_data_seg.first);
    client->poll_completion(conn);
    if (client->poll_completion(conn)) {
      s = Status::IOError("poll_completion failed");
    }
  }
  if (s.ok()) MarkTransAsFinished();

  // send raw data block allocated by trans_concurrent_arena
  delete[] metadata_;
  return s;
}

void SkipListRep::PackLocal(TransferService* node) const {
  LOG("SkipListRep::PackLocal");
  // int64_t msg = 0x1;
  // node->send(&msg, sizeof(msg));
  // ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>* ptr =
  //     skip_list_.Clone(protection_bytes_per_key);
  // ReadOnlySkipListRep::PackLocal(node, ptr);
  int64_t msg = 0x1;
  node->send(&msg, sizeof(msg));
  skip_list_.PackLocal(node);
  LOG("SkipListRep::PackLocal finish");
}

// caller async
void SkipListRep::MarkReadOnly() {
  LOG_CERR("SkipListRep MarkReadOnly");
  // PackLocal(TransferService *node, size_t protection_bytes_per_key)
  // todo: remote support
  MarkTransAsFinished();
}

}  // namespace

static std::unordered_map<std::string, OptionTypeInfo> skiplist_factory_info = {
    {"lookahead",
     {0, OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kDontSerialize /*Since it is part of the ID*/}},
};

SkipListFactory::SkipListFactory(size_t lookahead) : lookahead_(lookahead) {
  RegisterOptions("SkipListFactoryOptions", &lookahead_,
                  &skiplist_factory_info);
}

std::string SkipListFactory::GetId() const {
  std::string id = Name();
  if (lookahead_ > 0) {
    id.append(":").append(std::to_string(lookahead_));
  }
  return id;
}

MemTableRep* SkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  LOG("allocator::name: ", allocator->name());
  auto* ret = new SkipListRep(compare, allocator, transform, lookahead_);
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
