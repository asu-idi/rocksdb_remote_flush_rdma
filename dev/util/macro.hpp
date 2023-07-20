#pragma once
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <utility>

namespace Noncopyable {
class Noncopyable {
 public:
  Noncopyable() = default;
  virtual ~Noncopyable() = default;

  Noncopyable(const Noncopyable &) = delete;
  Noncopyable(Noncopyable &&) = delete;
  auto operator=(const Noncopyable &) noexcept -> Noncopyable & = delete;
  auto operator=(Noncopyable &&) noexcept -> Noncopyable & = delete;
};
}  // namespace Noncopyable

namespace Singleton {
template <class T>
class Singleton : public Noncopyable::Noncopyable {
 protected:
  Singleton() = default;
  ~Singleton() override = default;

 public:
  static auto Instance() -> T & {
    static T instance;
    return instance;
  }
};
}  // namespace Singleton
#define LOG(...)                                                     \
  Singleton::Singleton<LocalLogger::LocalLogger>::Instance().output( \
      std::this_thread::get_id(), __FILE__, __LINE__, __FUNCTION__,  \
      __VA_ARGS__);
