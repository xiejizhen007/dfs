#ifndef DFS_COMMON_UTILS_H
#define DFS_COMMON_UTILS_H

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/status.h"
#include "grpc++/grpc++.h"

namespace dfs {
namespace common {

template <class Key, class Value>
class parallel_hash_map {
   public:
    bool Contains(const Key& key) {
        absl::ReaderMutexLock lock_guard(&lock_);
        return map_.contains(key);
    }

    // if key already exist, return false and nothing happen
    // else return true, and insert <key, value> to hash_map
    bool TryInsert(const Key& key, const Value& value) {
        absl::WriterMutexLock lock_guard(&lock_);
        if (map_.contains(key)) {
            return false;
        }

        map_[key] = value;
        return true;
    }

    //
    std::pair<Value, bool> TryGet(const Key& key) {
        absl::ReaderMutexLock lock_guard(&lock_);
        if (!map_.contains(key)) {
            return {Value(), false};
        }

        return {map_.at(key), true};
    }

    // insert or update the <key, value>, even if the key does not exist
    void Set(const Key& key, const Value& value) {
        absl::WriterMutexLock lock_guard(&lock_);
        map_[key] = value;
    }

    // erase key from hash_map
    void Erase(const Key& key) {
        absl::WriterMutexLock lock_guard(&lock_);
        map_.erase(key);
    }

   private:
    absl::Mutex lock_;
    absl::flat_hash_map<Key, Value> map_;
};

google::protobuf::util::Status StatusGrpc2Protobuf(grpc::Status status);

grpc::Status StatusProtobuf2Grpc(google::protobuf::util::Status status);

}  // namespace common
}  // namespace dfs

#endif  // DFS_COMMON_UTILS_H