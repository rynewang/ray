// Copyright 2017 The Ray Authors.
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

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/dedicated_io_context_thread.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

// InMemoryStoreClient provides a thread-safe interface to an in-memory key-value store.
// All methods are posted to a dedicated thread to ensure thread safety.
// In the dedicated thread, a ThreadUnsafeInMemoryStoreClient is used to perform the
// actual operations, which is just a wrapper around an absl::flat_hash_map. The callbacks
// are posted to the callback_io_context.
//
// (all methods from all threads) -> post -> (ThreadUnsafeInMemoryStoreClient in dedicated
// thread) -> post -> (callbacks in callback_io_context)
class InMemoryStoreClient : public StoreClient {
 public:
  explicit InMemoryStoreClient(instrumented_io_context &callback_io_context);
  ~InMemoryStoreClient() = default;

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  const std::string &data,
                  bool overwrite,
                  std::function<void(bool)> callback) override;

  Status AsyncGet(const std::string &table_name,
                  const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const MapCallback<std::string, std::string> &callback) override;

  Status AsyncMultiGet(const std::string &table_name,
                       const std::vector<std::string> &keys,
                       const MapCallback<std::string, std::string> &callback) override;

  Status AsyncDelete(const std::string &table_name,
                     const std::string &key,
                     std::function<void(bool)> callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          std::function<void(int64_t)> callback) override;

  int GetNextJobID() override;

  Status AsyncGetKeys(const std::string &table_name,
                      const std::string &prefix,
                      std::function<void(std::vector<std::string>)> callback) override;

  Status AsyncExists(const std::string &table_name,
                     const std::string &key,
                     std::function<void(bool)> callback) override;

 private:
  class ThreadUnsafeInMemoryStoreClient {
   public:
    ThreadUnsafeInMemoryStoreClient() = default;
    ~ThreadUnsafeInMemoryStoreClient() = default;

    Status Put(const std::string &table_name,
               const std::string &key,
               const std::string &data,
               bool overwrite);
    Status Get(const std::string &table_name,
               const std::string &key,
               std::string *data) const;
    Status GetAll(const std::string &table_name,
                  absl::flat_hash_map<std::string, std::string> *data) const;
    Status MultiGet(const std::string &table_name,
                    const std::vector<std::string> &keys,
                    absl::flat_hash_map<std::string, std::string> *data) const;
    Status Delete(const std::string &table_name, const std::string &key);
    Status BatchDelete(const std::string &table_name,
                       const std::vector<std::string> &keys);
    int GetNextJobID();
    Status GetKeys(const std::string &table_name,
                   const std::string &prefix,
                   std::vector<std::string> *keys) const;
    Status Exists(const std::string &table_name,
                  const std::string &key,
                  bool *exists) const;

   private:
    // table_name -> key -> data
    absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, std::string>>
        store_;
    int next_job_id_ = 0;
  };

  ThreadUnsafeInMemoryStoreClient unsafe_client_;
  DedicatedIoContextThread dedicated_io_thread_;
  instrumented_io_context &callback_io_context_;
};

}  // namespace gcs

}  // namespace ray
