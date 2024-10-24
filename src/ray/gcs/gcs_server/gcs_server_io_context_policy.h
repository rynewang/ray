// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <type_traits>

#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/gcs/gcs_server/gcs_task_manager.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/util/array.h"

namespace ray {
namespace gcs {

struct GcsServerIoContextPolicy {
  GcsServerIoContextPolicy() = delete;

  // IoContext name for each handler.
  // If a class needs a dedicated io context, it should be specialized here.
  // If a class does NOT have a dedicated io context, returns -1;
  template <typename T>
  static constexpr int GetDedicatedIoContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return index_of("task_io_context");
    } else if constexpr (std::is_same_v<T, GcsPublisher>) {
      return index_of("pubsub_io_context");
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return index_of("ray_syncer_io_context");
    } else {
      // Due to if-constexpr limitations, this have to be in an else block.
      // Using this tuple_size_v to put T into compile error message.
      static_assert(std::tuple_size_v<std::tuple<T>> == 0, "unknown type");
    }
  }

  // This list must be unique and complete set of names returned from
  // GetDedicatedIoContextName. Or you can get runtime crashes when accessing a missing
  // name, or get leaks by creating unused threads.
  constexpr static std::array<std::string_view, 3> kAllDedicatedIoContextNames{
      "task_io_context", "pubsub_io_context", "ray_syncer_io_context"};

  constexpr static size_t index_of(std::string_view name) {
    return ray::index_of(kAllDedicatedIoContextNames, name);
  }
};

}  // namespace gcs
}  // namespace ray