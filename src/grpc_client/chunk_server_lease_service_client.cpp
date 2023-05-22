#include "src/grpc_client/chunk_server_lease_service_client.h"

#include "src/common/utils.h"

namespace dfs {
namespace grpc_client {

using dfs::common::StatusGrpc2Protobuf;
using protos::grpc::GrantLeaseRespond;
using protos::grpc::RevokeLeaseRespond;

google::protobuf::util::StatusOr<protos::grpc::GrantLeaseRespond>
ChunkServerLeaseServiceClient::SendRequest(
    const protos::grpc::GrantLeaseRequest& request) {
    grpc::ClientContext context;
    GrantLeaseRespond respond;
    auto status = stub_->GrantLease(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseRespond>
ChunkServerLeaseServiceClient::SendRequest(
    const protos::grpc::RevokeLeaseRequest& request) {
    grpc::ClientContext context;
    RevokeLeaseRespond respond;
    auto status = stub_->RevokeLease(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

}  // namespace grpc_client
}  // namespace dfs