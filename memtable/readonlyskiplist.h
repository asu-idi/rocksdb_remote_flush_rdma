//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.
//
// InlineSkipList is derived from SkipList (skiplist.h), but it optimizes
// the memory layout by requiring that the key storage be allocated through
// the skip list instance.  For the common case of SkipList<const char*,
// Cmp> this saves 1 pointer per skip list node and gives better cache
// locality, at the expense of wasted padding from using AllocateAligned
// instead of Allocate for the keys.  The unused padding will be from
// 0 to sizeof(void*)-1 bytes, and the space savings are sizeof(void*)
// bytes, so despite the padding the space used is always less than
// SkipList<const char*, ..>.
//
// Thread safety -------------
//
// Writes via Insert require external synchronization, most likely a mutex.
// InsertConcurrently can be safely called concurrently with reads and
// with other concurrent inserts.  Reads require a guarantee that the
// InlineSkipList will not be destroyed while the read is in progress.
// Apart from that, reads progress without any internal locking or
// synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the InlineSkipList is
// destroyed.  This is trivially guaranteed by the code since we never
// delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the InlineSkipList.
// Only Insert() modifies the list, and it is careful to initialize a
// node and use release-stores to publish the nodes in one or more lists.
//
// ... prev vs. next pointer ordering ...
//

#pragma once
#include <assert.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ios>
#include <type_traits>

#include "db/memtable.h"
#include "memory/allocator.h"
#include "memory/trans_concurrent_arena.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/logger.hpp"
#include "rocksdb/remote_flush_service.h"
#include "rocksdb/remote_transfer_service.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

template <class Comparator>
class InlineSkipList;
template <class Comparator>
class TransInlineSkipList;

template <class Comparator>
class ReadOnlyInlineSkipList {
 public:
  void PackLocal(TransferService* node) const;
  static void* UnPackLocal(TransferService* node);
  void check_data() const;

 public:
  using DecodedKey =
      typename std::remove_reference<Comparator>::type::DecodedType;
  explicit ReadOnlyInlineSkipList(const InlineSkipList<Comparator>&,
                                  const Comparator cmp,
                                  size_t protection_bytes_per_key);
  explicit ReadOnlyInlineSkipList(const TransInlineSkipList<Comparator>&,
                                  const Comparator cmp,
                                  size_t protection_bytes_per_key);
  ReadOnlyInlineSkipList() = default;
  ~ReadOnlyInlineSkipList() {
    if (data_ != nullptr) {
      LOG("destruct ReadOnlyInlineSkipList");
      free(data_);
    }
  }
  ReadOnlyInlineSkipList(const ReadOnlyInlineSkipList&) = delete;
  ReadOnlyInlineSkipList& operator=(const ReadOnlyInlineSkipList&) = delete;

  class Iterator {
   public:
    explicit Iterator(const ReadOnlyInlineSkipList* list);
    void SetList(const ReadOnlyInlineSkipList* list);
    bool Valid() const;
    const char* key() const;
    void Next();
    void Prev();
    void Seek(const char* target);
    void SeekToFirst();

   private:
    const ReadOnlyInlineSkipList* list_;
    void* now_;
  };

 private:
  inline static const size_t magic = 0xdeadbeef;
  // Comparator const compare_;
  void* data_ = nullptr;
  int64_t total_len_ = 0;
};

template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::check_data() const {
  Iterator iter(this);
  iter.SeekToFirst();
  while (iter.Valid()) {
    size_t key_len = 0;
    char* ptr = const_cast<char*>(iter.key());
    ptr -= sizeof(size_t);
    memcpy(&key_len, ptr, sizeof(size_t));
    iter.Next();
  }
}

template <class Comparator>
inline void* ReadOnlyInlineSkipList<Comparator>::UnPackLocal(
    TransferService* node) {
  LOG("unpack ReadOnlyInlineSkipList");
  // void* local_cmp_ = MemTable::KeyComparator::UnPackLocal(sockfd);
  int64_t* total_len = nullptr;
  size_t size = sizeof(total_len);
  node->receive(reinterpret_cast<void**>(&total_len), &size);
  void* data = malloc(*total_len);
  node->receive(&data, reinterpret_cast<size_t*>(total_len));
  auto* local_readonly_skiplistrep = new ReadOnlyInlineSkipList();
  void* mem = reinterpret_cast<void*>(local_readonly_skiplistrep);
  node->receive(mem, sizeof(ReadOnlyInlineSkipList));
  LOG("ReadOnlyInlineSkipList::UnPackLocal read mem len:",
      sizeof(ReadOnlyInlineSkipList));
  local_readonly_skiplistrep->data_ = data;
  local_readonly_skiplistrep->total_len_ = *total_len;
  LOG("unpack ReadOnlyInlineSkipList done");
  return mem;
}

template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::PackLocal(
    TransferService* node) const {
  LOG("pack ReadOnlyInlineSkipList");
  // compare_.PackLocal(sockfd);
  LOG("ReadOnlyInlineSkipList::PackLocal send tot_len start:", total_len_);
  node->send(reinterpret_cast<const void*>(&total_len_), sizeof(total_len_));
  LOG("ReadOnlyInlineSkipList::PackLocal send tot_len:", total_len_);
  node->send(data_, total_len_);
  node->send(reinterpret_cast<const void*>(this),
             sizeof(ReadOnlyInlineSkipList));
  LOG("ReadOnlyInlineSkipList::PackLocal send data len:",
      sizeof(ReadOnlyInlineSkipList));
  LOG("pack ReadOnlyInlineSkipList done");
}

template <class Comparator>
inline ReadOnlyInlineSkipList<Comparator>::Iterator::Iterator(
    const ReadOnlyInlineSkipList* list) {
  SetList(list);
}

template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::Iterator::SetList(
    const ReadOnlyInlineSkipList* list) {
  list_ = list;
}

template <class Comparator>
inline bool ReadOnlyInlineSkipList<Comparator>::Iterator::Valid() const {
  if (now_ == nullptr) return false;
  size_t key_len = 0;
  char* ptr = reinterpret_cast<char*>(now_);
  memcpy(&key_len, ptr, sizeof(size_t));
  return key_len != magic;
}

template <class Comparator>
inline const char* ReadOnlyInlineSkipList<Comparator>::Iterator::key() const {
  assert(Valid());
  size_t key_len = 0;
  char* ptr = reinterpret_cast<char*>(now_);
  memcpy(&key_len, ptr, sizeof(size_t));
  ptr += sizeof(size_t);
  return ptr;
}
template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::Iterator::Next() {
  if (!Valid()) {
    now_ = nullptr;
    return;
  }
  size_t key_len = 0;
  char* ptr = reinterpret_cast<char*>(now_);
  memcpy(&key_len, ptr, sizeof(size_t));
  ptr += (2 * sizeof(size_t) + key_len);
  now_ = reinterpret_cast<void*>(ptr);
}
template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::Iterator::Prev() {
  if (!Valid()) {
    now_ = nullptr;
    return;
  }
  size_t key_len = 0;
  char* ptr = reinterpret_cast<char*>(now_);
  ptr -= sizeof(size_t);
  memcpy(&key_len, ptr, sizeof(size_t));
  ptr -= ((key_len != magic) ? (key_len + sizeof(size_t)) : 0);
  now_ = reinterpret_cast<void*>(ptr);
  return;
}

template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::Iterator::Seek(
    const char* target) {
  assert(false);
}

template <class Comparator>
inline void ReadOnlyInlineSkipList<Comparator>::Iterator::SeekToFirst() {
  char* ptr = reinterpret_cast<char*>(list_->data_);
  size_t magic_val = 0;
  memcpy(&magic_val, ptr, sizeof(size_t));
  assert(magic_val == magic);
  ptr += sizeof(size_t);
  now_ = reinterpret_cast<void*>(ptr);
}

template <class Comparator>
ReadOnlyInlineSkipList<Comparator>::ReadOnlyInlineSkipList(
    const InlineSkipList<Comparator>& raw_skip_list_, const Comparator cmp,
    size_t protection_bytes_per_key) {
  LOG("construct ReadOnlyInlineSkipList");
  typename InlineSkipList<Comparator>::Iterator iter(&raw_skip_list_);
  int64_t total_len = 0;
  iter.SeekToFirst();
  while (iter.Valid()) {
    const char* key_ptr = iter.key();
    size_t key_len = cmp.decode_len(key_ptr, protection_bytes_per_key);
    total_len += (int64_t)(key_len + 2 * sizeof(size_t));
    iter.Next();
  }
  data_ = malloc(total_len + 2 * sizeof(size_t));
  total_len_ = sizeof(size_t) * 2 + total_len;
  char* data_ptr = reinterpret_cast<char*>(data_);
  memcpy(data_ptr, &magic, sizeof(size_t));
  data_ptr += sizeof(size_t);
  iter.SeekToFirst();
  while (iter.Valid()) {
    const char* key_ptr = iter.key();
    size_t key_len = cmp.decode_len(key_ptr, protection_bytes_per_key);
    memcpy(data_ptr, &key_len, sizeof(size_t));
    data_ptr += sizeof(size_t);
    memcpy(data_ptr, key_ptr, key_len);
    data_ptr += key_len;
    memcpy(data_ptr, &key_len, sizeof(size_t));
    data_ptr += sizeof(size_t);
    LOG_CERR("key_len:", key_len, ' ', key_ptr);
    iter.Next();
  }
  memcpy(data_ptr, &magic, sizeof(size_t));
  LOG("construct ReadOnlyInlineSkipList done");
}

template <class Comparator>
ReadOnlyInlineSkipList<Comparator>::ReadOnlyInlineSkipList(
    const TransInlineSkipList<Comparator>& raw_skip_list_, const Comparator cmp,
    size_t protection_bytes_per_key) {
  LOG("construct ReadOnlyInlineSkipList");
  typename TransInlineSkipList<Comparator>::Iterator iter(&raw_skip_list_);
  int64_t total_len = 0;
  iter.SeekToFirst();
  while (iter.Valid()) {
    const char* key_ptr = iter.key();
    size_t key_len = cmp.decode_len(key_ptr, protection_bytes_per_key);
    total_len += (int64_t)(key_len + 2 * sizeof(size_t));
    iter.Next();
  }
  data_ = malloc(total_len + 2 * sizeof(size_t));
  total_len_ = sizeof(size_t) * 2 + total_len;
  char* data_ptr = reinterpret_cast<char*>(data_);
  memcpy(data_ptr, &magic, sizeof(size_t));
  data_ptr += sizeof(size_t);
  iter.SeekToFirst();
  while (iter.Valid()) {
    const char* key_ptr = iter.key();
    size_t key_len = cmp.decode_len(key_ptr, protection_bytes_per_key);
    memcpy(data_ptr, &key_len, sizeof(size_t));
    data_ptr += sizeof(size_t);
    memcpy(data_ptr, key_ptr, key_len);
    data_ptr += key_len;
    memcpy(data_ptr, &key_len, sizeof(size_t));
    data_ptr += sizeof(size_t);
    LOG_CERR("key_len:", key_len, ' ', key_ptr);
    iter.Next();
  }
  memcpy(data_ptr, &magic, sizeof(size_t));
  LOG("construct ReadOnlyInlineSkipList done");
}

}  // namespace ROCKSDB_NAMESPACE