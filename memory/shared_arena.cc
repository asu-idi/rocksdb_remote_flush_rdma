#include "memory/shared_arena.h"

#include "util/logger.hpp"
namespace ROCKSDB_NAMESPACE {
SharedArena::SharedArena(size_t block_size, AllocTracker* tracker)
    : tracker_(tracker) {
  assert(tracker_ == nullptr);
}
SharedArena::~SharedArena() { mp_blocks_.clear(); }

}  // namespace ROCKSDB_NAMESPACE