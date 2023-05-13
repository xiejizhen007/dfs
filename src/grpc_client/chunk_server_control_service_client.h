#ifndef DFS_GRPC_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H
#define DFS_GRPC_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H

#include <memory>

#include "chunk_server_control_service.grpc.pb.h"
#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"

namespace dfs {
namespace grpc_client {

class ChunkServerControlServiceClient {
   public:
    ChunkServerControlServiceClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(protos::grpc::ChunkServerControlService::NewStub(channel)) {}

    google::protobuf::util::Status SendHeartBeat(
        const protos::grpc::SendHeartBeatRequest& request);

   private:
    std::unique_ptr<protos::grpc::ChunkServerControlService::Stub> stub_;
};

}  // namespace grpc_client
}  // namespace dfs

#endif  // DFS_GRPC_CLIENT_CHUNK_SERVER_CONTROL_SERVICE_CLIENT_H