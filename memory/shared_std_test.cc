#include "memory/shared_std.hpp"

#include <cassert>

#include "db/db_iter.h"
#include "gtest/gtest.h"
#include "port/stack_trace.h"
namespace ROCKSDB_NAMESPACE {

class SharedStdTest : public testing::Test {};

TEST_F(SharedStdTest, Vector) {
  shm_std::shared_list<int*> now1, now2;
  for (int i = 1; i <= 10; i++) {
    int* ptr = new int(i * 2);
    now1.push_back(ptr);
  }
  now1 = now2;
  ASSERT_TRUE(now1.size() == now2.size());
  for (int i = 1; i <= 10; i++) {
    ASSERT_TRUE(now1.front() == now2.front());
    now1.pop_front();
    now2.pop_front();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
