#ifndef DFS_SERVER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H

#include "chunk_server_manager_service.grpc.pb.h"

namespace dfs {
namespace server {

class ChunkServerManagerServiceImpl final
    : public protos::grpc::ChunkServerManagerService::Service {
   public:
    grpc::Status ReportChunkServer(
        grpc::ServerContext* context,
        const protos::grpc::ReportChunkServerRequest* request,
        protos::grpc::ReportChunkServerRespond* respond) override;

   private:
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_MANAGER_SERVICE_IMPL_H