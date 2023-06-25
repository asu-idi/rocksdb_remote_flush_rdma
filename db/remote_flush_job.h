//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class RemoteFlushJob {
 public:
  static RemoteFlushJob* CreateRemoteFlushJob(
      const std::string& dbname, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
      const FileOptions& file_options, VersionSet* versions,
      InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
      std::vector<SequenceNumber> existing_snapshots,
      SequenceNumber earliest_write_conflict_snapshot, JobContext* job_context,
      FlushReason flush_reason, FSDirectory* db_directory,
      FSDirectory* output_file_directory, CompressionType output_compression,
      bool measure_io_stats, const bool sync_output_directory,
      const bool write_manifest, Env::Priority thread_pri,
      const SeqnoToTimeMapping& seq_time_mapping, const std::string& db_id = "",
      const std::string& db_session_id = "",
      BlobFileCompletionCallback* blob_callback = nullptr);

 private:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while RemoteFlushJob is
  // alive
  RemoteFlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                 const ImmutableDBOptions& db_options,
                 const MutableCFOptions& mutable_cf_options,
                 uint64_t max_memtable_id, const FileOptions& file_options,
                 VersionSet* versions, InstrumentedMutex* db_mutex,
                 std::atomic<bool>* shutting_down,
                 std::vector<SequenceNumber> existing_snapshots,
                 SequenceNumber earliest_write_conflict_snapshot,
                 JobContext* job_context, FlushReason flush_reason,
                 FSDirectory* db_directory, FSDirectory* output_file_directory,
                 CompressionType output_compression, bool measure_io_stats,
                 const bool sync_output_directory, const bool write_manifest,
                 Env::Priority thread_pri,
                 const SeqnoToTimeMapping& seq_time_mapping,
                 const std::string& db_id = "",
                 const std::string& db_session_id = "",
                 BlobFileCompletionCallback* blob_callback = nullptr);

 public:
  ~RemoteFlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  Status RunLocal(LogsWithPrepTracker* prep_tracker = nullptr,
                  FileMetaData* file_meta = nullptr,
                  bool* switched_to_mempurge = nullptr);
  Status RunRemote(LogsWithPrepTracker* prep_tracker = nullptr,
                   FileMetaData* file_meta = nullptr,
                   bool* switched_to_mempurge = nullptr);
  void Cancel();
  const autovector<MemTable*>& GetMemTables() const { return mems_; }

  std::list<std::unique_ptr<FlushJobInfo>>* GetCommittedRemoteFlushJobsInfo() {
    return &committed_flush_jobs_info_;
  }
  bool CHECKShared();

 private:
  friend class RemoteFlushJobTest_GetRateLimiterPriorityForWrite_Test;

  void ReportStartedFlush();
  void ReportFlushInputSize(const autovector<MemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table();
  Status MemPurge();
  bool MemPurgeDecider(double threshold);
  Env::IOPriority GetRateLimiterPriorityForWrite();
  std::unique_ptr<FlushJobInfo> GetRemoteFlushJobInfo() const;

  // encode needed
  const std::string& dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  // truncate needed
  VersionSet* versions_;  // partly transfered
  VersionEdit* edit_;
  Version* base_;
  // easy to shared
  uint64_t max_memtable_id_;
  std::atomic<bool>* shutting_down_;  // shared
  std::vector<SequenceNumber> existing_snapshots_;
  SequenceNumber earliest_write_conflict_snapshot_;
  FlushReason flush_reason_;
  CompressionType output_compression_;
  bool measure_io_stats_;
  const bool sync_output_directory_;
  const bool write_manifest_;
  bool pick_memtable_called;
  Env::Priority thread_pri_;

  // data structure
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  const FileOptions file_options_;
  JobContext* job_context_;
  FSDirectory* db_directory_;           // shared
  FSDirectory* output_file_directory_;  // shared
  TableProperties table_properties_;    // maybe no need to share
  FileMetaData meta_;
  std::list<std::unique_ptr<FlushJobInfo>> committed_flush_jobs_info_;
  autovector<MemTable*> mems_;
  const SeqnoToTimeMapping& db_impl_seqno_time_mapping_;
  SeqnoToTimeMapping seqno_to_time_mapping_;
  SystemClock* clock_;  // shared

  // no need to shared
  InstrumentedMutex* db_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
