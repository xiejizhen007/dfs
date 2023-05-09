#ifndef DFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H

#include <memory>
#include <string>
#include <stack>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/stubs/statusor.h"

namespace dfs {
namespace server {

// single instance
class LockManager {
   public:
    static LockManager* GetInstance();

    bool ExistLock(const std::string& filename);

    google::protobuf::util::StatusOr<absl::Mutex*> CreateLock(
        const std::string& filename);

    google::protobuf::util::StatusOr<absl::Mutex*> FetchLock(
        const std::string& filename);

   private:
    absl::flat_hash_map<std::string, std::shared_ptr<absl::Mutex>>
        filepath_locks_;
    absl::Mutex lock_;
};


// 对于文件 /a/b/c，ParentLocks 依次对 a，b 进行加锁
// 并将锁放入栈中，以便在析构时对 b，a 进行解锁
class ParentLocks {
public:
    ParentLocks(LockManager* lock_manager, const std::string& filename);
    ~ParentLocks();

    bool ok() const;
    google::protobuf::util::Status status() const;
    size_t lock_size() const;
private:
    std::stack<absl::Mutex*> locks_;
    google::protobuf::util::Status status_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_LOCK_MANAGER_H