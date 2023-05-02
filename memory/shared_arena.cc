#include "memory/shared_arena.h"

#include "util/logger.hpp"
namespace ROCKSDB_NAMESPACE {
SharedArena::SharedArena(size_t block_size, AllocTracker* tracker)
    : tracker_(tracker) {
  LOG("default block size=", block_size, ", currently unused");
  assert(tracker_ == nullptr);
}
SharedArena::~SharedArena() {
  LOG("release shared arena");
  for (auto& v : mp_blocks_) {
    LOG("release mp_blocks_: ", v.first, ' ', v.second.first.get(), ' ',
        v.second.second);
  }

  mp_blocks_.clear();
}

}  // namespace ROCKSDB_NAMESPACE