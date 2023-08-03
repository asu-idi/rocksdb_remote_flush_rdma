//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "memory/remote_flush_service.h"
#ifdef __linux__
#include <sys/socket.h>
#endif

#include "memory/remote_flush_service.h"
#include "rocksdb/table.h"
#include "util/gflags_compat.h"
#include "util/random.h"

DECLARE_int32(mark_for_compaction_one_file_in);

namespace ROCKSDB_NAMESPACE {

// A `DbStressTablePropertiesCollector` ignores what keys/values were added to
// the table, adds no properties to the table, and decides at random whether the
// table will be marked for compaction according to
// `FLAGS_mark_for_compaction_one_file_in`.
class DbStressTablePropertiesCollector : public TablePropertiesCollector {
 public:
  DbStressTablePropertiesCollector()
      : need_compact_(Random::GetTLSInstance()->OneInOpt(
            FLAGS_mark_for_compaction_one_file_in)) {}

  virtual Status AddUserKey(const Slice& /* key */, const Slice& /* value */,
                            EntryType /*type*/, SequenceNumber /*seq*/,
                            uint64_t /*file_size*/) override {
    return Status::OK();
  }

  virtual Status Finish(UserCollectedProperties* /* properties */) override {
    return Status::OK();
  }

  virtual UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }

  virtual const char* Name() const override {
    return "DbStressTablePropertiesCollector";
  }

  virtual bool NeedCompact() const override { return need_compact_; }

 private:
  const bool need_compact_;
};

// A `DbStressTablePropertiesCollectorFactory` creates
// `DbStressTablePropertiesCollectorFactory`s.
class DbStressTablePropertiesCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  void PackLocal(TCPNode* node) const override {
    size_t msg_len = sizeof(size_t) + sizeof(size_t) * 2 + sizeof(double);
    char* msg = reinterpret_cast<char*>(malloc(msg_len));
    *reinterpret_cast<size_t*>(msg) = 1;
    node->send(msg, msg_len);
  }
  void PackLocal(char*& buf) const override {
    size_t msg_len = sizeof(size_t) + sizeof(size_t) * 2 + sizeof(double);
    char* msg = reinterpret_cast<char*>(malloc(msg_len));
    *reinterpret_cast<size_t*>(msg) = 1;
    PACK_TO_BUF(msg, buf, msg_len);
  }

 public:
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /* context */) override {
    return new DbStressTablePropertiesCollector();
  }

  virtual const char* Name() const override {
    return "DbStressTablePropertiesCollectorFactory";
  }
};

}  // namespace ROCKSDB_NAMESPACE
