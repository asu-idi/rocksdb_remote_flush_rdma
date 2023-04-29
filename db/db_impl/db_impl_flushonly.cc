#include "db/db_impl/db_impl_flushonly.h"

#include <string>

#include "db/arena_wrapped_db_iter.h"
#include "db/db_impl/compacted_db_impl.h"
#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/job_context.h"
#include "db/merge_context.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/options.h"
#include "util/cast_util.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {
DBImplFlushOnly::DBImplFlushOnly(const DBOptions& db_options,
                                 const std::string& dbname)
    : DBImpl(db_options, dbname, false, true, true /*read-only*/) {
  LOG("CALL Flushonly db constructor");

  LogFlush(immutable_db_options_.info_log);  // DEBUG
}
DBImplFlushOnly::~DBImplFlushOnly() {}
// Implementations of the DB interface
Status DBImplFlushOnly::Get(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableSlice* pinnable_val) {
  return Get(read_options, column_family, key, pinnable_val,
             /*timestamp*/ nullptr);
}
Status DBImplFlushOnly::Get(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            PinnableSlice* pinnable_val,
                            std::string* timestamp) {
  assert(pinnable_val != nullptr);
  // TODO: stopwatch DB_GET needed?, perf timer needed?
  PERF_TIMER_GUARD(get_snapshot_time);

  assert(column_family);
  if (read_options.timestamp) {
    const Status s = FailIfTsMismatchCf(
        column_family, *(read_options.timestamp), /*ts_for_read=*/true);
    if (!s.ok()) {
      return s;
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamp) {
    timestamp->clear();
  }

  const Comparator* ucmp = column_family->GetComparator();
  assert(ucmp);
  std::string* ts = ucmp->timestamp_size() > 0 ? timestamp : nullptr;

  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  GetWithTimestampReadCallback read_cb(snapshot);
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      tracer_->Get(column_family, key);
    }
  }
  SuperVersion* super_version = cfd->GetSuperVersion();
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);
  if (super_version->mem->Get(lkey, pinnable_val->GetSelf(),
                              /*columns=*/nullptr, ts, &s, &merge_context,
                              &max_covering_tombstone_seq, read_options,
                              false /* immutable_memtable */, &read_cb)) {
    pinnable_val->PinSelf();
    RecordTick(stats_, MEMTABLE_HIT);
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    PinnedIteratorsManager pinned_iters_mgr;
    super_version->current->Get(
        read_options, lkey, pinnable_val, /*columns=*/nullptr, ts, &s,
        &merge_context, &max_covering_tombstone_seq, &pinned_iters_mgr,
        /*value_found*/ nullptr,
        /*key_exists*/ nullptr, /*seq*/ nullptr, &read_cb,
        /*is_blob*/ nullptr,
        /*do_merge*/ true);
    RecordTick(stats_, MEMTABLE_MISS);
  }
  RecordTick(stats_, NUMBER_KEYS_READ);
  size_t size = pinnable_val->size();
  RecordTick(stats_, BYTES_READ, size);
  RecordInHistogram(stats_, BYTES_PER_READ, size);
  PERF_COUNTER_ADD(get_read_bytes, size);
  return s;
}

Iterator* DBImplFlushOnly::NewIterator(const ReadOptions& read_options,
                                       ColumnFamilyHandle* column_family) {
  assert(column_family);
  if (read_options.timestamp) {
    const Status s = FailIfTsMismatchCf(
        column_family, *(read_options.timestamp), /*ts_for_read=*/true);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                ->number_
          : latest_snapshot;
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  auto db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(), super_version->mutable_cf_options,
      super_version->current, read_seq,
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number, read_callback);
  auto internal_iter = NewInternalIterator(
      db_iter->GetReadOptions(), cfd, super_version, db_iter->GetArena(),
      read_seq, /* allow_unprepared_value */ true, db_iter);
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplFlushOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.timestamp) {
    for (auto* cf : column_families) {
      assert(cf);
      const Status s = FailIfTsMismatchCf(cf, *(read_options.timestamp),
                                          /*ts_for_read=*/true);
      if (!s.ok()) {
        return s;
      }
    }
  } else {
    for (auto* cf : column_families) {
      assert(cf);
      const Status s = FailIfCfHasTs(cf);
      if (!s.ok()) {
        return s;
      }
    }
  }

  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                ->number_
          : latest_snapshot;

  for (auto cfh : column_families) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
    auto* sv = cfd->GetSuperVersion()->Ref();
    auto* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
        sv->current, read_seq,
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number, read_callback);
    auto* internal_iter = NewInternalIterator(
        db_iter->GetReadOptions(), cfd, sv, db_iter->GetArena(), read_seq,
        /* allow_unprepared_value */ true, db_iter);
    db_iter->SetIterUnderDBIter(internal_iter);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

namespace {}

Status DBImplFlushOnly::OpenForFlushOnlyWithoutCheck(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles /*out*/, DB** dbptr, /*out*/
    bool error_if_wal_file_exists) {
  *dbptr = nullptr;
  handles->clear();

  auto* impl = new DBImplFlushOnly(db_options, dbname);

  impl->mutex_.Lock();
  //   impl->column_family_memtables_;
  impl->mutex_.Unlock();
  *dbptr = impl;
  //   for (auto* h : *handles) {
  //     impl->NewThreadStatusCfInfo(
  //         static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
  //   }
}
}  // namespace ROCKSDB_NAMESPACE