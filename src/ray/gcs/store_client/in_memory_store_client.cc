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

#include "ray/gcs/store_client/in_memory_store_client.h"

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

namespace ray {
namespace gcs {

// This class provides a synchronous, thread-unsafe implementation of an in-memory store
// client. It should only be accessed from a single thread or with external
// synchronization.
class InMemoryStoreClient::ThreadUnsafeInMemoryStoreClient {
 public:
  ThreadUnsafeInMemoryStoreClient() : next_job_id_(0) {}

  Status Put(const std::string &table_name,
             const std::string &key,
             const std::string &data,
             bool overwrite) {
    auto &table = store_[table_name];
    if (!overwrite && table.contains(key)) {
      return Status::KeyExists("Key already exists");
    }
    table[key] = data;
    return Status::OK();
  }

  Status Get(const std::string &table_name,
             const std::string &key,
             std::optional<std::string> &out) const {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      out = std::nullopt;
      return Status::OK();
    }
    const auto &table = table_it->second;
    auto it = table.find(key);
    if (it == table.end()) {
      out = std::nullopt;
    } else {
      out = it->second;
    }
    return Status::OK();
  }

  Status GetAll(const std::string &table_name,
                absl::flat_hash_map<std::string, std::string> &out) const {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      out.clear();
    } else {
      out = table_it->second;
    }
    return Status::OK();
  }

  Status MultiGet(const std::string &table_name,
                  const std::vector<std::string> &keys,
                  absl::flat_hash_map<std::string, std::string> &out) const {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      out.clear();
      return Status::OK();
    }
    const auto &table = table_it->second;
    for (const auto &key : keys) {
      auto it = table.find(key);
      if (it != table.end()) {
        out[key] = it->second;
      }
    }
    return Status::OK();
  }

  Status Delete(const std::string &table_name, const std::string &key) {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      return Status::KeyNotFound("Table not found");
    }
    if (table_it->second.erase(key) == 0) {
      return Status::KeyNotFound("Key not found");
    }
    return Status::OK();
  }

  Status BatchDelete(const std::string &table_name,
                     const std::vector<std::string> &keys) {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      return Status::KeyNotFound("Table not found");
    }
    auto &table = table_it->second;
    for (const auto &key : keys) {
      table.erase(key);
    }
    return Status::OK();
  }

  int GetNextJobID() { return next_job_id_++; }

  Status GetKeys(const std::string &table_name,
                 const std::string &prefix,
                 std::vector<std::string> &out) const {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      return Status::KeyNotFound("Table not found");
    }
    const auto &table = table_it->second;
    for (const auto &pair : table) {
      if (pair.first.compare(0, prefix.length(), prefix) == 0) {
        out.push_back(pair.first);
      }
    }
    return Status::OK();
  }

  Status Exists(const std::string &table_name, const std::string &key, bool &out) const {
    auto table_it = store_.find(table_name);
    if (table_it == store_.end()) {
      out = false;
    } else {
      out = table_it->second.contains(key);
    }
    return Status::OK();
  }

 private:
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, std::string>> store_;
  std::atomic<int> next_job_id_;
};

template <typename Func, typename Callback, typename... Args>
Status InMemoryStoreClient::PostToIOThread(Func &&func,
                                           Callback &&callback,
                                           Args &&...args) {
  dedicated_io_thread_.GetIOContext().post([this,
                                            f = std::forward<Func>(func),
                                            cb = std::forward<Callback>(callback),
                                            ... args =
                                                std::forward<Args>(args)]() mutable {
    auto result = std::invoke(f, unsafe_client_.get(), std::forward<Args>(args)...);
    callback_io_context_.post([cb = std::move(cb), result = std::move(result)]() mutable {
      std::invoke(std::move(cb), std::move(result));
    });
  });
  return Status::OK();
}

Status InMemoryStoreClient::AsyncPut(const std::string &table_name,
                                     const std::string &key,
                                     const std::string &data,
                                     bool overwrite,
                                     std::function<void(Status)> callback) {
  return PostToIOThread(&ThreadUnsafeInMemoryStoreClient::Put,
                        std::move(callback),
                        table_name,
                        key,
                        data,
                        overwrite);
}

Status InMemoryStoreClient::AsyncGet(const std::string &table_name,
                                     const std::string &key,
                                     const OptionalItemCallback<std::string> &callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::Get,
      [callback](Status status, std::optional<std::string> value) {
        callback(std::move(value));
      },
      table_name,
      key);
}

Status InMemoryStoreClient::AsyncGetAll(
    const std::string &table_name,
    const MapCallback<std::string, std::string> &callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::GetAll,
      [callback](Status status, absl::flat_hash_map<std::string, std::string> value) {
        callback(std::move(value));
      },
      table_name);
}

Status InMemoryStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    const MapCallback<std::string, std::string> &callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::MultiGet,
      [callback](Status status, absl::flat_hash_map<std::string, std::string> value) {
        callback(std::move(value));
      },
      table_name,
      keys);
}

Status InMemoryStoreClient::AsyncDelete(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(Status)> callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::Delete, std::move(callback), table_name, key);
}

Status InMemoryStoreClient::AsyncBatchDelete(const std::string &table_name,
                                             const std::vector<std::string> &keys,
                                             std::function<void(Status)> callback) {
  return PostToIOThread(&ThreadUnsafeInMemoryStoreClient::BatchDelete,
                        std::move(callback),
                        table_name,
                        keys);
}

int InMemoryStoreClient::GetNextJobID() { return unsafe_client_->GetNextJobID(); }

Status InMemoryStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    std::function<void(std::vector<std::string>)> callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::GetKeys,
      [callback](Status status, std::vector<std::string> keys) {
        callback(std::move(keys));
      },
      table_name,
      prefix);
}

Status InMemoryStoreClient::AsyncExists(const std::string &table_name,
                                        const std::string &key,
                                        std::function<void(bool)> callback) {
  return PostToIOThread(
      &ThreadUnsafeInMemoryStoreClient::Exists,
      [callback](Status status, bool exists) { callback(exists); },
      table_name,
      key);
}

}  // namespace gcs
}  // namespace ray
