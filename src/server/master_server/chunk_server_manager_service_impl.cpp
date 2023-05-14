#include "src/server/master_server/chunk_server_manager_service_impl.h"

#include "src/server/master_server/chunk_server_manager.h"

namespace dfs {
namespace server {

grpc::Status ChunkServerManagerServiceImpl::ReportChunkServer(
    grpc::ServerContext* context,
    const protos::grpc::ReportChunkServerRequest* request,
    protos::grpc::ReportChunkServerRespond* respond) {
    // 从 request 中获取 chunk_server 信息
    auto info = request->chunk_server();
    auto chunk_server =
        ChunkServerManager::GetInstance()->GetChunkServer(info.location());
    if (!chunk_server) {
        std::shared_ptr<protos::ChunkServer> new_chunk_server(
            new protos::ChunkServer(info));
        ChunkServerManager::GetInstance()->RegisterChunkServer(
            new_chunk_server);
    } else {
    }
}

}  // namespace server
}  // namespace dfs