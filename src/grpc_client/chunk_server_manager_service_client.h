#ifndef DFS_GRPC_CLIENT_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H
#define DFS_GRPC_CLIENT_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H

#include <google/protobuf/stubs/statusor.h>
#include <grpcpp/grpcpp.h>

#include <memory>

#include "chunk_server_manager_service.grpc.pb.h"

namespace dfs {
namespace grpc_client {

class ChunkServerManagerServiceClient {
   public:
    ChunkServerManagerServiceClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(protos::grpc::ChunkServerManagerService::NewStub(channel)) {}

    google::protobuf::util::StatusOr<protos::grpc::ReportChunkServerRespond>
    SendRequest(const protos::grpc::ReportChunkServerRequest& request);

   private:
    std::unique_ptr<protos::grpc::ChunkServerManagerService::Stub> stub_;
};

}  // namespace grpc_client
}  // namespace dfs

#endif  // DFS_GRPC_CLIENT_CHUNK_SERVER_MANAGER_SERVICE_CLIENT_H