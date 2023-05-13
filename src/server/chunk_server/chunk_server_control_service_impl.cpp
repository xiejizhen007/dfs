#include "src/server/chunk_server/chunk_server_control_service_impl.h"

namespace dfs {
namespace server {

grpc::Status ChunkServerControlServiceImpl::SendHeartBeat(
    grpc::ServerContext* context,
    const protos::grpc::SendHeartBeatRequest* request,
    protos::grpc::SendHeartBeatRespond* respond) {
    // check the server is alive
    *respond->mutable_request() = *request;
    return grpc::Status::OK;
}

}  // namespace server
}  // namespace dfs