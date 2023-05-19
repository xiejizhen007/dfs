#ifndef DFS_GRPC_CLIENT_CHUNK_SERVER_FILE_SERVICE_CLIENT_H
#define DFS_GRPC_CLIENT_CHUNK_SERVER_FILE_SERVICE_CLIENT_H

#include <google/protobuf/stubs/statusor.h>
#include <grpcpp/grpcpp.h>

#include <memory>

#include "chunk_server_file_service.grpc.pb.h"

namespace dfs {
namespace grpc_client {

class ChunkServerFileServiceClient {
   public:
    ChunkServerFileServiceClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(protos::grpc::ChunkServerFileService::NewStub(channel)) {}

    google::protobuf::util::StatusOr<protos::grpc::InitFileChunkRespond>
    SendRequest(const protos::grpc::InitFileChunkRequest& request);

    google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
    SendRequest(const protos::grpc::ReadFileChunkRequest& request);

    google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkRespond>
    SendRequest(const protos::grpc::WriteFileChunkRequest& request);

    google::protobuf::util::StatusOr<protos::grpc::SendChunkDataRespond>
    SendRequest(const protos::grpc::SendChunkDataRequest& request);

   private:
    std::unique_ptr<protos::grpc::ChunkServerFileService::Stub> stub_;
};

}  // namespace grpc_client
}  // namespace dfs

#endif  // DFS_GRPC_CLIENT_CHUNK_SERVER_FILE_SERVICE_CLIENT_H