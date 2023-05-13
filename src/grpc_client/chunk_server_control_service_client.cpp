#include "src/grpc_client/chunk_server_control_service_client.h"

namespace dfs {
namespace grpc_client {

google::protobuf::util::Status ChunkServerControlServiceClient::SendHeartBeat(
    const protos::grpc::SendHeartBeatRequest& request) {
    grpc::ClientContext context;
    protos::grpc::SendHeartBeatRespond respond;

    auto status = stub_->SendHeartBeat(&context, request, &respond);
    if (!status.ok()) {
        return google::protobuf::util::UnknownError(status.error_message());
    }

    return google::protobuf::util::OkStatus();
}

}  // namespace grpc_client
}  // namespace dfs