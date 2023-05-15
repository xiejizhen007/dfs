#include "src/grpc_client/master_metadata_service_client.h"

#include "src/common/utils.h"

namespace dfs {
namespace grpc_client {

using dfs::common::StatusGrpc2Protobuf;
using protos::grpc::OpenFileRespond;

google::protobuf::util::StatusOr<protos::grpc::OpenFileRespond>
MasterMetadataServiceClient::SendRequest(
    const protos::grpc::OpenFileRequest& request) {
    grpc::ClientContext context;
    OpenFileRespond respond;

    auto status = stub_->OpenFile(&context, request, &respond);
    if (!status.ok()) {
        return StatusGrpc2Protobuf(status);
    }

    return respond;
}

}  // namespace grpc_client
}  // namespace dfs