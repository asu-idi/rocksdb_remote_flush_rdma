#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "db/version_edit.h"
#include "env/file_system_tracer.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/listener.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#ifdef __linux
#include <sys/ipc.h>
#include <sys/shm.h>
#endif  // __linux
namespace ROCKSDB_NAMESPACE {
class Statistics;
class SystemClock;

// write memtable file content to CXL mem instead of directly writing to file
class CXLMemoryWriter {
 private:
  //  notify through EventListeners
  std::string fname_;
  std::string dump_file_name_;
  SystemClock* clock_;
  std::atomic_uint64_t mem_size_;
  Statistics* stats_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::unique_ptr<FileChecksumGenerator> checksum_generator_;
  bool checksum_finalized_;
  uint32_t buffered_data_crc32c_checksum_;
  bool buffered_data_with_checksum_;
  void* raw_mem_ptr_;

 public:
  CXLMemoryWriter(
      std::string _cxl_file_name, std::string _dump_file_name,
      SystemClock* clock = nullptr, Statistics* stats = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      FileChecksumGenFactory* file_checksum_gen_factory = nullptr,
      bool buffered_data_with_checksum = false)
      : fname_(std::move(_cxl_file_name)),
        dump_file_name_(std::move(_dump_file_name)),
        clock_(clock),
        mem_size_(0),
        stats_(stats),
        listeners_(),
        checksum_generator_(nullptr),
        checksum_finalized_(false),
        buffered_data_crc32c_checksum_(0),
        buffered_data_with_checksum_(buffered_data_with_checksum),
        raw_mem_ptr_(nullptr) {
    std::for_each(listeners.begin(), listeners.end(),
                  [this](const std::shared_ptr<EventListener>& e) {
                    if (e->ShouldBeNotifiedOnFileIO()) {
                      listeners_.emplace_back(e);
                    }
                  });
    if (file_checksum_gen_factory) {
      FileChecksumGenContext checksum_gen_context;
      checksum_gen_context.file_name = dump_file_name_;
      checksum_generator_ =
          file_checksum_gen_factory->CreateFileChecksumGenerator(
              checksum_gen_context);
    }
    // shm
  }
  std::string sst_file_name() const { return dump_file_name_; }
  [[maybe_unused]] IOStatus Append(
      const Slice& data, uint32_t crc32c_checksum = 0,
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);
  [[maybe_unused]] IOStatus Pad(
      const size_t pad_types,
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);
  [[maybe_unused]] IOStatus Flush(
      Env::IOPriority op_rate_limiter_priority = Env::IO_TOTAL);
  [[maybe_unused]] IOStatus Close();
  uint64_t GetMemSize() const {
    return mem_size_.load(std::memory_order_acquire);
  }
  std::string GetFileCheckSum();
  [[nodiscard]] const char* GetFileChecksumName() const;
};
}  // namespace ROCKSDB_NAMESPACE