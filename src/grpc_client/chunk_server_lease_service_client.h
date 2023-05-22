#ifndef DFS_GRPC_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H
#define DFS_GRPC_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H

#include <google/protobuf/stubs/statusor.h>
#include <grpcpp/grpcpp.h>

#include <memory>

#include "chunk_server_lease_service.grpc.pb.h"

namespace dfs {
namespace grpc_client {

class ChunkServerLeaseServiceClient {
   public:
    ChunkServerLeaseServiceClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(protos::grpc::ChunkServerLeaseService::NewStub(channel)) {}

    google::protobuf::util::StatusOr<protos::grpc::GrantLeaseRespond>
    SendRequest(const protos::grpc::GrantLeaseRequest& request);

    google::protobuf::util::StatusOr<protos::grpc::RevokeLeaseRespond>
    SendRequest(const protos::grpc::RevokeLeaseRequest& request);

   private:
    std::unique_ptr<protos::grpc::ChunkServerLeaseService::Stub> stub_;
};

}  // namespace grpc_client
}  // namespace dfs

#endif  // DFS_GRPC_CLIENT_CHUNK_SERVER_LEASE_SERVICE_CLIENT_H