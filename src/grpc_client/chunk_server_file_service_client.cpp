#include "src/grpc_client/chunk_server_file_service_client.h"

#include "src/common/utils.h"

namespace dfs {
namespace grpc_client {

using dfs::common::StatusGrpc2Protobuf;
using protos::grpc::AdjustFileChunkVersionRespond;
using protos::grpc::ApplyMutationRespond;
using protos::grpc::InitFileChunkRespond;
using protos::grpc::ReadFileChunkRespond;
using protos::grpc::SendChunkDataRespond;
using protos::grpc::WriteFileChunkRespond;

google::protobuf::util::StatusOr<protos::grpc::InitFileChunkRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::InitFileChunkRequest& request) {
    grpc::ClientContext context;
    InitFileChunkRespond respond;
    auto status = stub_->InitFileChunk(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::ReadFileChunkRequest& request) {
    grpc::ClientContext context;
    ReadFileChunkRespond respond;
    auto status = stub_->ReadFileChunk(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::WriteFileChunkRequest& request) {
    grpc::ClientContext context;
    WriteFileChunkRespond respond;
    auto status = stub_->WriteFileChunk(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::SendChunkDataRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::SendChunkDataRequest& request) {
    grpc::ClientContext context;
    SendChunkDataRespond respond;
    auto status = stub_->SendChunkData(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::ApplyMutationRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::ApplyMutationRequest& request) {
    grpc::ClientContext context;
    ApplyMutationRespond respond;
    auto status = stub_->ApplyMutation(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

google::protobuf::util::StatusOr<protos::grpc::AdjustFileChunkVersionRespond>
ChunkServerFileServiceClient::SendRequest(
    const protos::grpc::AdjustFileChunkVersionRequest& request) {
    grpc::ClientContext context;
    AdjustFileChunkVersionRespond respond;
    auto status = stub_->AdjustFileChunkVersion(&context, request, &respond);
    if (status.ok()) {
        return respond;
    }
    return StatusGrpc2Protobuf(status);
}

}  // namespace grpc_client
}  // namespace dfs