#include "db/retrieve_info.h"

#include <cassert>
#include <cstring>

#include "env/emulated_clock.h"
#include "rocksdb/env.h"
#include "rocksdb/system_clock.h"
namespace ROCKSDB_NAMESPACE {
SystemClock* retrieve_from(const std::string& clock_info_) {
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