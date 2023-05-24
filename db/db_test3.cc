//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#include <fcntl.h>

#include <algorithm>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>

#ifndef OS_WIN
#include <unistd.h>
#endif
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include "cache/lru_cache.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/experimental.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "table/mock_table.h"
#include "table/scoped_arena_iterator.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/compression.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Note that whole DBTest and its child classes disable fsync on files
// and directories for speed.
// If fsync needs to be covered in a test, put it in other places.
class DBTest3 : public DBTestBase {
 public:
  DBTest3() : DBTestBase("db_test", /*env_do_fsync=*/true) {}
};

class DBTestWithParam
    : public DBTest3,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBTestWithParam() {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

TEST_F(DBTest3, ShmMockEnvTest) {
  // Options options;
  // options.allow_concurrent_memtable_write = false;
  // options.create_if_missing = true;
  // options.env = env_;
  // options.server_use_remote_flush = true;
  Options options;
  options.allow_concurrent_memtable_write = false;
  options.create_if_missing = true;
  std::unique_ptr<MockEnv> env{MockEnv::Create(Env::Default())};
  options.env = env.get();
  Close();
  last_options_.table_factory.reset();
  last_options_ = options;
  DB* db = nullptr;
  DB::Open(options, dbname_, &db);

  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};
  LOG("Start put some kv");
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }
  LOG("Finish put some kv,Start get kv");
  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
  LOG("Finish get kv, Start check iterator");
  Iterator* iterator = db->NewIterator(ReadOptions());
  LOG("iterator seek to first");
  iterator->SeekToFirst();
  LOG("iterator seek all");
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  LOG("iterator delete");
  delete iterator;

  DBImpl* dbi = static_cast_with_check<DBImpl>(db);
  LOG("Check flush memtable");
  ASSERT_OK(dbi->TEST_FlushMemTable());
  LOG("finish flush,Start recheck kv");
  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    LOG("");
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    LOG("");
    ASSERT_TRUE(res == vals[i]);
    LOG("");
  }
  LOG("Finish recheck kv, delete db");
  delete db;
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
