#include "memory/shared_std.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <random>

#include "db/db_iter.h"
#include "gtest/gtest.h"
#include "memory/shared_mem_basic.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "util/macro.hpp"
namespace ROCKSDB_NAMESPACE {

class SharedStdTest : public testing::Test {};

TEST_F(SharedStdTest, DISABLED_Vector) {
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

TEST_F(SharedStdTest, DISABLED_Set) {
  std::set<std::pair<void*, size_t>> mpp;
  for (int i = 1; i <= 10; i++) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(1, INT32_MAX - 1);
    size_t* ptr = nullptr;
    size_t size = dis(gen);
    // ptr = (size_t*)dis(gen);
    mpp.insert(std::make_pair(ptr, size));
  }
  ASSERT_TRUE(mpp.size() == 10);
  void* ptr = nullptr;
  size_t size = 0;
  for (auto& it : mpp) {
    ASSERT_TRUE(it.first >= ptr);
    if (it.first == ptr) ASSERT_TRUE(it.second >= size);
    ptr = it.first;
    size = it.second;
  }
}
TEST_F(SharedStdTest, List) {
  shm_std::shared_list<int*> all;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dis(1, INT32_MAX - 1);
  int* num[11] = {nullptr};
  for (int i = 1; i <= 10; i++) {
    num[i] = reinterpret_cast<int*>(dis(gen));
    LOG("num", std::hex, num[i]);
    all.push_front(num[i]);
  }
  singleton<SharedContainer>::Instance().debug();
  ASSERT_TRUE(all.size() == 10);
  singleton<SharedContainer>::Instance().debug();
  for (auto& ptr : all) {
    LOG("ptr", std::hex, &ptr, ' ', ptr);
    ASSERT_TRUE(singleton<SharedContainer>::Instance().find(&ptr, sizeof(int)));
  }
  for (int i = 1; i <= 10; i++) {
    LOG("&all.front(), sizeof(int))", &all.front(), ' ', sizeof(int));
    ASSERT_TRUE(
        singleton<SharedContainer>::Instance().find(&all.front(), sizeof(int)));
    all.pop_front();
  }
}

TEST_F(SharedStdTest, DISABLED_Map) {
  std::map<void*, size_t> mpp;
  for (int i = 1; i <= 10; i++) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(1, INT32_MAX - 1);
    size_t* ptr = (size_t*)dis(gen);
    size_t size = dis(gen);
    mpp.insert(std::make_pair(ptr, size));
  }
  ASSERT_TRUE(mpp.size() == 10);
  void* ptr = nullptr;
  size_t size = 0;
  for (auto& it : mpp) {
    ASSERT_TRUE(it.first >= ptr);
    if (it.first == ptr) ASSERT_TRUE(it.second >= size);
    ptr = it.first;
    size = it.second;
  }
}

TEST_F(SharedStdTest, DISABLED_SharedContainer) {
  std::vector<std::pair<size_t*, size_t>> mpp;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dis(1, 1e5);
  size_t *ptr = nullptr, size = 0;
  for (int i = 1; i <= 10; i++) {
    ptr += dis(gen);
    size = dis(gen);
    ASSERT_TRUE(singleton<SharedContainer>::Instance().insert(ptr, size));
    ASSERT_FALSE(singleton<SharedContainer>::Instance().insert(ptr, size));
    mpp.emplace_back(ptr, size);
    ptr += size;
  }
  ASSERT_EQ(singleton<SharedContainer>::Instance().size(), 10);
  for (auto& it : mpp) {
    ASSERT_TRUE(
        singleton<SharedContainer>::Instance().find(it.first, it.second));
  }
  for (auto& it : mpp) {
    ASSERT_TRUE(singleton<SharedContainer>::Instance().remove(it.first));
  }
  ASSERT_EQ(singleton<SharedContainer>::Instance().size(), 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
