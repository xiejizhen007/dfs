#include "lock_manager.h"

namespace dfs {
namespace server {

using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;

LockManager* LockManager::GetInstance() {
    static LockManager* instance = new LockManager();
    return instance;
}

bool LockManager::ExistLock(const std::string& filename) {
    return filepath_locks_.Contains(filename);
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::CreateLock(
    const std::string& filename) {
    std::shared_ptr<absl::Mutex> file_lock(new absl::Mutex());

    if (!filepath_locks_.TryInsert(filename, file_lock)) {
        return google::protobuf::util::AlreadyExistsError(
            "Lock already exists for " + filename);
    }

    return file_lock.get();
}

google::protobuf::util::StatusOr<absl::Mutex*> LockManager::FetchLock(
    const std::string& filename) {
    auto value_pair = filepath_locks_.TryGet(filename);
    if (!value_pair.second) {
        return google::protobuf::util::NotFoundError("Lock not found for " +
                                                     filename);
    }

    return value_pair.first.get();
}

ParentLocks::ParentLocks(LockManager* lock_manager,
                         const std::string& filename) {
    auto findPos = filename.find('/', 1);
    while (findPos != std::string::npos) {
        auto curDir = filename.substr(0, findPos);

        auto curDirLockOr = lock_manager->FetchLock(curDir);
        if (!curDirLockOr.ok()) {
            status_ = NotFoundError("Lock for " + curDir + " does not exist");
            return;
        }

        auto curDirLock = curDirLockOr.value();
        curDirLock->ReaderLock();
        locks_.push(curDirLock);

        findPos = filename.find('/', findPos + 1);
    }

    status_ = OkStatus();
}

ParentLocks::~ParentLocks() {
    while (!locks_.empty()) {
        auto lock = locks_.top();
        locks_.pop();
        lock->ReaderUnlock();
    }
}

bool ParentLocks::ok() const { return status_.ok(); }

google::protobuf::util::Status ParentLocks::status() const { return status_; }

size_t ParentLocks::lock_size() const { return locks_.size(); }

}  // namespace server
}  // namespace dfs