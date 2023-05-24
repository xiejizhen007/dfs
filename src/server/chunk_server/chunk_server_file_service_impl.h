#ifndef DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H

#include "chunk_server_file_service.grpc.pb.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace dfs {
namespace server {

class ChunkServerFileServiceImpl final
    : public protos::grpc::ChunkServerFileService::Service {
   public:
    // 主服务器调用，用于创建数据块
    grpc::Status InitFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::InitFileChunkRequest* request,
        protos::grpc::InitFileChunkRespond* respond) override;

    // 客户端调用，用于读取数据块
    grpc::Status ReadFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::ReadFileChunkRequest* request,
        protos::grpc::ReadFileChunkRespond* respond) override;

    // 客户端调用，写入数据块
    grpc::Status WriteFileChunk(
        grpc::ServerContext* context,
        const protos::grpc::WriteFileChunkRequest* request,
        protos::grpc::WriteFileChunkRespond* respond) override;

    // 客户端调用，将数据与校验和发送至块服务器
    grpc::Status SendChunkData(
        grpc::ServerContext* context,
        const protos::grpc::SendChunkDataRequest* request,
        protos::grpc::SendChunkDataRespond* respond) override;

    // 主副本服务器调用，将数据写入至其他的副本服务器
    grpc::Status ApplyMutation(
        grpc::ServerContext* context,
        const protos::grpc::ApplyMutationRequest* request,
        protos::grpc::ApplyMutationRespond* respond) override;

    // 主服务器调用，更新数据块版本号
    grpc::Status AdjustFileChunkVersion(
        grpc::ServerContext* context,
        const protos::grpc::AdjustFileChunkVersionRequest* request,
        protos::grpc::AdjustFileChunkVersionRespond* respond) override;

   private:
    FileChunkManager* file_chunk_manager();

    grpc::Status WriteFileChunkLocally(
        const protos::grpc::WriteFileChunkRequestHeader& header,
        protos::grpc::WriteFileChunkRespond* respond);
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_FILE_SERVICE_IMPL_H