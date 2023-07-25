#include <cassert>
#include <cstddef>
#include <iterator>
#include <thread>

#include "db/remote_flush_job.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
SystemClock* retrieve_from(const std::string& clock_info_);
}