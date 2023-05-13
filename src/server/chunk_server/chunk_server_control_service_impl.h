#ifndef DFS_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H

#include "chunk_server_control_service.grpc.pb.h"

namespace dfs {
namespace server {

// chunk server send heartbeat package to master server, telling master that
// chunk server is still alive
class ChunkServerControlServiceImpl final
    : public protos::grpc::ChunkServerControlService::Service {
   public:
    grpc::Status SendHeartBeat(
        grpc::ServerContext* context,
        const protos::grpc::SendHeartBeatRequest* request,
        protos::grpc::SendHeartBeatRespond* respond) override;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_CONTROL_SERVICE_IMPL_H