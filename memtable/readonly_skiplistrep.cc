#include <chrono>
#include <cstddef>
#include <random>
#include <thread>

#include "inlineskiplist.h"
#include "memtable/readonly_skiplisrep.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
namespace ROCKSDB_NAMESPACE {

ReadOnlySkipListRep::ReadOnlySkipListRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const ReadOnlyInlineSkipList<const MemTableRep::KeyComparator&>*
        readonly_skiplistrep)
    // TODO: maybe need to use ConSharedArena
    : MemTableRep(allocator),
      read_only_skip_list_(readonly_skiplistrep),
      cmp_(compare),
      lookahead_(0) {
  LOG("Construct Rd skiplistRep");
  readonly_skiplistrep->CHECK_all_addr();
  LOG("Construct Rd skiplistRep init");
}
void ReadOnlySkipListRep::Get(const LookupKey& k, void* callback_args,
                              bool (*callback_func)(void* arg,
                                                    const char* entry)) {
  ReadOnlySkipListRep::Iterator iter(this->read_only_skip_list_);
  Slice dummy_slice;
  for (iter.Seek(dummy_slice, k.memtable_key().data());
       iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
  }
}

}  // namespace ROCKSDB_NAMESPACE