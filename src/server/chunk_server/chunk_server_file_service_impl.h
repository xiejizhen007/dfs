#ifndef DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H

#include "chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace dfs {
namespace server {

class ChunkServerFileServiceImpl final
    : public protos::grpc::ChunkServerFileService::Service {
   public:
    grpc::Status InitFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::InitFileChunkRequest* request,
        protos::grpc::InitFileChunkRespond* respond) override;

    grpc::Status ReadFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::ReadFileChunkRequest* request,
        protos::grpc::ReadFileChunkRespond* respond) override;

    grpc::Status WriteFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::WriteFileChunkRequest* request,
        protos::grpc::WriteFileChunkRespond* respond) override;

   private:
    FileChunkManager* file_chunk_manager();

    grpc::Status WriteFileChunkLocally(
        const protos::grpc::WriteFileChunkRequestHeader& header,
        protos::grpc::WriteFileChunkRespond* respond);
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H