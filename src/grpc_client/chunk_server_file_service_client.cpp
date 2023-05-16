#include "src/grpc_client/chunk_server_file_service_client.h"

#include "src/common/utils.h"

namespace dfs {
namespace grpc_client {

using dfs::common::StatusGrpc2Protobuf;
using protos::grpc::ReadFileChunkRespond;
using protos::grpc::WriteFileChunkRespond;

google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::ReadFileChunkRequest& request) {
    grpc::ClientContext context;
    ReadFileChunkRespond respond;
    auto status = stub_->ReadFileChunk(&context, request, &respond);
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::WriteFileChunkRequest& request) {
    grpc::ClientContext context;
    WriteFileChunkRespond respond;
    auto status = stub_->WriteFileChunk(&context, request, &respond);
    return StatusGrpc2Protobuf(status);
}

}  // namespace grpc_client
}  // namespace dfs