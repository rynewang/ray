#pragma once

#include <boost/asio.hpp>
#include <thread>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/util/util.h"

namespace ray {
namespace common {

class DedicatedIoContextThread {
 public:
  explicit DedicatedIoContextThread(std::string thread_name)
      : work_(io_service_), thread_name_(std::move(thread_name)) {
    io_thread_ = std::thread([this] {
      SetThreadName(thread_name_);
      io_service_.run();
    });
  }

  ~DedicatedIoContextThread() {
    io_service_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

  instrumented_io_context &GetIoService() { return io_service_; }

 private:
  instrumented_io_context io_service_;
  boost::asio::io_service::work work_;  // to keep io_service_ running
  std::thread io_thread_;
  std::string thread_name_;
};

}  // namespace common
}  // namespace ray
