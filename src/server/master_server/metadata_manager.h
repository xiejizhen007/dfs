#ifndef DFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H

#include <atomic>

#include "absl/container/flat_hash_map.h"
#include "google/protobuf/stubs/statusor.h"
#include "metadata.pb.h"
#include "src/common/utils.h"
#include "src/server/master_server/lock_manager.h"

namespace dfs {
namespace server {

using protos::FileMetadata;

class MetadataManager {
   public:
    static MetadataManager* GetInstance();

    bool ExistFileMetadata(const std::string& filename);

    bool ExistFileChunkMetadata(const std::string& chunk_handle);

    google::protobuf::util::Status CreateFileMetadata(
        const std::string& filename);

    google::protobuf::util::StatusOr<std::shared_ptr<FileMetadata>>
    GetFileMetadata(const std::string& filename);

    google::protobuf::util::StatusOr<std::string> CreateChunkHandle(
        const std::string& filename, uint32_t chunk_index);

    google::protobuf::util::StatusOr<std::string> GetChunkHandle(
        const std::string& filename, uint32_t chunk_index);

    google::protobuf::util::StatusOr<protos::FileChunkMetadata>
    GetFileChunkMetadata(const std::string& chunk_handle);

    google::protobuf::util::Status IncFileChunkVersion(
        const std::string& chunk_handle);

    void SetFileChunkMetadata(const protos::FileChunkMetadata& metadata);

    void DeleteFileAndChunkMetadata(const std::string& filename);

    void SetLeaseMetadata(const std::string& chunk_handle,
                          const std::string& client_url,
                          uint64_t expire_time);

    void RemoveLeaseMetadata(const std::string& chunk_handle);

    std::pair<std::pair<std::string, uint64_t>, bool>
    GetLeaseMetadata(const std::string& chunk_handle);

   private:
    MetadataManager();

    // allocate a new chunk handle, which is chunk uuid.
    std::string AllocateNewChunkHandle();

    LockManager* lock_manager_;

    // key: filename
    dfs::common::parallel_hash_map<std::string,
                                   std::shared_ptr<protos::FileMetadata>>
        file_metadatas_;

    // key: chunk_handle
    dfs::common::parallel_hash_map<std::string, protos::FileChunkMetadata>
        chunk_metadatas_;

    // <chunk_handle, <client url, expire time>>
    // 确保客户端对数据块写入的独占性
    dfs::common::parallel_hash_map<std::string,
                                   std::pair<std::string, uint64_t>>
        chunk_leases_;

    // 用于给每个 chunk 分配 uuid
    std::atomic<uint64_t> global_chunk_id_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_METADATA_MANAGER_H