#include "metadata_manager.h"

namespace dfs {
namespace server {

using google::protobuf::util::AlreadyExistsError;
using google::protobuf::util::IsAlreadyExists;
using google::protobuf::util::IsNotFound;
using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;

using protos::FileMetadata;

MetadataManager::MetadataManager() : global_chunk_id_(0) {
    lock_manager_ = LockManager::GetInstance();
}

MetadataManager* MetadataManager::GetInstance() {
    static MetadataManager* instance = new MetadataManager();
    return instance;
}

bool MetadataManager::ExistFileMetadata(const std::string& filename) {
    return file_metadatas_.contains(filename);
}

google::protobuf::util::Status MetadataManager::CreateFileMetadata(
    const std::string& filename) {
    // step 1: Lock the parent directory first(readerlock)
    ParentLocks plocks(lock_manager_, filename);
    if (!plocks.ok()) {
        return plocks.status();
    }

    // step 2: Create a new lock for this new file(writelock)
    auto path_lock_or = lock_manager_->CreateLock(filename);
    if (!path_lock_or.ok()) {
        if (IsAlreadyExists(path_lock_or.status())) {
            // re-create?
            path_lock_or = lock_manager_->FetchLock(filename);
        } else {
            return path_lock_or.status();
        }
    }

    absl::WriterMutexLock path_lock_or_guard(path_lock_or.value());

    // step 3: Instantiate and initialize file metadata
    auto new_file_metadata(std::make_shared<FileMetadata>());
    new_file_metadata->set_filename(filename);

    absl::WriterMutexLock lock_guard(&lock_);
    if (file_metadatas_.contains(filename)) {
        return AlreadyExistsError(filename + " metadata is already exist");
    }

    file_metadatas_[filename] = new_file_metadata;

    return OkStatus();
}

google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>>
MetadataManager::GetFileMetadata(const std::string& filename) {
    absl::ReaderMutexLock lock_guard(&lock_);
    if (!file_metadatas_.contains(filename)) {
        return NotFoundError(filename + " metadata not found");
    }

    return file_metadatas_[filename];
}

google::protobuf::util::StatusOr<std::string>
MetadataManager::CreateChunkHandle(const std::string& filename,
                                   uint32_t chunk_index) {
    std::string new_chunk_handle;
    {
        // 给 file 的上级目录上 readerlock
        ParentLocks plocks(lock_manager_, filename);
        if (!plocks.ok()) {
            return plocks.status();
        }

        // 给当前文件上 writerlock
        auto cur_path_lock_or = lock_manager_->FetchLock(filename);
        if (!cur_path_lock_or.ok()) {
            return cur_path_lock_or.status();
        }

        absl::WriterMutexLock(cur_path_lock_or.value());

        // 获取 file 的 metadata
        auto file_metadata_or = GetFileMetadata(filename);
        if (!file_metadata_or.ok()) {
            return file_metadata_or.status();
        }

        auto file_metadata = file_metadata_or.value();

        // 申请一个新的 chunk handle
        new_chunk_handle = AllocateNewChunkHandle();
        file_metadata->set_filename(filename);

        auto& chunk_handles = (*file_metadata->mutable_chunk_handles());
        if (chunk_handles.contains(chunk_index)) {
            return google::protobuf::util::AlreadyExistsError(
                "chunk " + std::to_string(chunk_index) +
                " is already exist in file " + filename);
        }

        // 将键值 <chunk_index, chunk_handle> 插入 map 表中
        chunk_handles[chunk_index] = new_chunk_handle;
        count_.fetch_add(1);
    }

    // 设置好新的 chunk_metadata，插入 map<chunk_handle, chunk_metadata> 中
    protos::FileChunkMetadata chunk_metadata;
    chunk_metadata.set_chunk_handle(new_chunk_handle);
    SetFileChunkMetadata(chunk_metadata);

    return new_chunk_handle;
}

google::protobuf::util::StatusOr<std::string> MetadataManager::GetChunkHandle(
    const std::string& filename, uint32_t chunk_index) {
    // 获取上级目录的 readerlock
    ParentLocks plocks(lock_manager_, filename);
    if (!plocks.ok()) {
        return plocks.status();
    }

    // 获取当前文件的 readerlock
    auto file_lock_or = lock_manager_->FetchLock(filename);
    if (!file_lock_or.ok()) {
        return file_lock_or.status();
    }
    absl::ReaderMutexLock file_lock_guard(file_lock_or.value());

    // 从 metadata 中获得 chunk_handle
    auto file_metadata_or = GetFileMetadata(filename);
    if (!file_metadata_or.ok()) {
        return file_metadata_or.status();
    }

    auto file_metadata = file_metadata_or.value();
    const auto& chunk_handles = file_metadata->chunk_handles();
    if (!chunk_handles.contains(chunk_index)) {
        return google::protobuf::util::NotFoundError(
            "file " + filename + " not found chunk " +
            std::to_string(chunk_index));
    }

    return chunk_handles.at(chunk_index);
}

google::protobuf::util::StatusOr<protos::FileChunkMetadata>
MetadataManager::GetFileChunkMetadata(const std::string& chunk_handle) {
    absl::ReaderMutexLock lock_guard(&lock_);
    if (!chunk_metadatas_.contains(chunk_handle)) {
        return google::protobuf::util::NotFoundError(
            "chunk_handle metadata does not exist");
    }

    return chunk_metadatas_.at(chunk_handle);
}

google::protobuf::util::Status MetadataManager::IncFileChunkVersion(
    const std::string& chunk_handle) {
    auto chunk_metadata_or = GetFileChunkMetadata(chunk_handle);
    if (!chunk_metadata_or.ok()) {
        return chunk_metadata_or.status();
    }

    auto chunk_metadata = chunk_metadata_or.value();
    chunk_metadata.set_version(chunk_metadata.version() + 1);
    SetFileChunkMetadata(chunk_metadata);

    return google::protobuf::util::OkStatus();
}

void MetadataManager::SetFileChunkMetadata(
    const protos::FileChunkMetadata& metadata) {
    // 将 metadata 注册到 chunk_metadatas_ 中
    absl::WriterMutexLock lock_guard(&lock_);
    const std::string& chunk_handle = metadata.chunk_handle();
    chunk_metadatas_[chunk_handle] = metadata;
}

void MetadataManager::DeleteFileAndChunkMetadata(const std::string& filename) {
    absl::WriterMutexLock lock_guard(&lock_);
    file_metadatas_.erase(filename);
    chunk_metadatas_.erase(filename);
}

std::string MetadataManager::AllocateNewChunkHandle() {
    return std::to_string(global_chunk_id_.fetch_add(1));
}

}  // namespace server
}  // namespace dfs