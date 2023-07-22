#pragma once
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstddef>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "memory/concurrent_arena.h"
#include "memory/concurrent_shared_arena.h"
#include "rocksdb/write_buffer_manager.h"

namespace ROCKSDB_NAMESPACE {
class BasicArenaFactory {
 public:
  static BasicArena* UnPackLocal(int sockfd) {
    std::string msg;
    msg.resize(15);
    read(sockfd, msg.data(), 15);
    if (msg.substr(0, 5) == "Arena") {
      return reinterpret_cast<Arena*>(Arena::UnPackLocal(sockfd));
    } else if (msg.substr(0, 15) ==
               std::string("ConcurrentArena").substr(0, 15)) {
      return reinterpret_cast<ConcurrentArena*>(
          ConcurrentArena::UnPackLocal(sockfd));
    } else if (msg.substr(0, 15) ==
               std::string("ConcurrentSharedArena").substr(0, 15)) {
      return reinterpret_cast<ConSharedArena*>(
          // should use ConSharedArena
          ConSharedArena::UnPackLocal(sockfd));
    } else {
      LOG("BasicArenaFactory::UnPackLocal: error: ", msg, ' ', msg.substr(0, 5),
          ' ', msg.substr(0, 15));

      assert(false);
      return nullptr;
    }
  }

 private:
  BasicArenaFactory() = default;
  BasicArenaFactory(const BasicArenaFactory&) = delete;
  void operator=(const BasicArenaFactory&) = delete;
  ~BasicArenaFactory() = default;
};

}  // namespace ROCKSDB_NAMESPACE