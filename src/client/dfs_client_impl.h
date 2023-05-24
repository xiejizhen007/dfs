#ifndef DFS_CLIENT_DFS_CLIENT_IMPL_H
#define DFS_CLIENT_DFS_CLIENT_IMPL_H

#include <google/protobuf/stubs/statusor.h>

#include <string>

#include "master_metadata_service.grpc.pb.h"
#include "src/client/client_cache_manager.h"
#include "src/common/config_manager.h"
#include "src/common/utils.h"
#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/grpc_client/master_metadata_service_client.h"

namespace dfs {
namespace client {

// 实现 client 对文件的基本操作
class DfsClientImpl {
   public:
    DfsClientImpl();

    google::protobuf::util::Status CreateFile(const std::string& filename);

    google::protobuf::util::Status DeleteFile(const std::string& filename);

    google::protobuf::util::StatusOr<std::pair<size_t, std::string>> ReadFile(
        const std::string& filename, size_t offset, size_t nbytes);

    google::protobuf::util::StatusOr<size_t> WriteFile(
        const std::string& filename, const std::string& data, size_t offset,
        size_t nbytes);

   private:
    // cache metadata to cache manager
    void CacheToCacheManager(const std::string& filename,
                             const uint32_t& chunk_index,
                             const protos::grpc::OpenFileRespond& respond);

    google::protobuf::util::Status GetChunkMetedata(
        const std::string& filename, const size_t& chunk_index,
        const protos::grpc::OpenFileRequest::OpenMode& openmode,
        std::string& chunk_handle, size_t& chunk_version,
        CacheManager::ChunkServerLocationEntry& entry);

    google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
    ReadFileChunk(const std::string& filename, size_t chunk_index,
                  size_t offset, size_t nbytes);

    google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkRespond>
    WriteFileChunk(const std::string& filename, const std::string& data,
                   size_t chunk_index, size_t offset, size_t nbytes);

    std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
    GetChunkServerFileServiceClient(const std::string& address);

    bool RegisterChunkServerFileServiceClient(const std::string& address);

    dfs::common::parallel_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>>
        chunk_server_file_service_clients_;

    std::shared_ptr<dfs::grpc_client::MasterMetadataServiceClient>
        master_metadata_service_client_;

    std::shared_ptr<CacheManager> cache_manager_;

    dfs::common::ConfigManager* config_manager_;
};

}  // namespace client
}  // namespace dfs

#endif  // DFS_CLIENT_DFS_CLIENT_IMPL_H