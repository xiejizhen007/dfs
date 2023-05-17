#include "src/grpc_client/chunk_server_manager_service_client.h"

#include "src/common/utils.h"

namespace dfs {
namespace grpc_client {

using dfs::common::StatusGrpc2Protobuf;
using protos::grpc::ReportChunkServerRespond;

google::protobuf::util::StatusOr<protos::grpc::ReportChunkServerRespond>
ChunkServerManagerServiceClient::SendRequest(
    const protos::grpc::ReportChunkServerRequest& request) {
    grpc::ClientContext context;
    ReportChunkServerRespond respond;
    auto status = stub_->ReportChunkServer(&context, request, &respond);
    if (!status.ok()) {
        return StatusGrpc2Protobuf(status);
    }
    return respond;
}

}  // namespace grpc_client
}  // namespace dfs