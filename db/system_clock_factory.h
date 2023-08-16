#include <cassert>
#include <cstddef>
#include <iterator>
#include <thread>

#include "db/remote_flush_job.h"
#include "env/emulated_clock.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
class SystemClockFactory {
 public:
  static void* UnPackLocal(TransferService* node);

  SystemClockFactory(const SystemClockFactory&) = delete;
  void operator=(const SystemClockFactory&) = delete;

 private:
  SystemClockFactory() = default;
  ~SystemClockFactory() = default;
};
inline void* SystemClockFactory::UnPackLocal(TransferService* node) {
  std::string clock_info_;
  clock_info_.resize(20);
  node->receive(clock_info_.data(), 20);

  if (strncmp(clock_info_.c_str(), "SystemClock", strlen("SystemClock")) == 0 ||
      strncmp(clock_info_.c_str(), "DefaultClock", strlen("DefaultClock")) ==
          0) {
    return SystemClock::Default().get();
  }
  if (strncmp(clock_info_.c_str(), "LegacySystemClock",
              strlen("LegacySystemClock")) == 0)
    return Env::Default()->GetSystemClock().get();
  if (strncmp(clock_info_.c_str(), "TimeEmulatedSystemClock",
              strlen("TimeEmulatedSystemClock")) == 0 ||
      strncmp(clock_info_.c_str(), "TimeEmulatedSystemCl",
              strlen("TimeEmulatedSystemCl")) == 0)
    return new EmulatedSystemClock(SystemClock::Default());
  LOG("Unknown clock info: ", clock_info_.c_str());
  assert(false);
}
}  // namespace ROCKSDB_NAMESPACE