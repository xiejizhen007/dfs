#ifndef DFS_GRPC_CLIENT_MASTER_METADATA_SERVICE_CLIENT_H
#define DFS_GRPC_CLIENT_MASTER_METADATA_SERVICE_CLIENT_H

#include <memory>

#include "google/protobuf/stubs/statusor.h"
#include "grpcpp/grpcpp.h"
#include "master_metadata_service.grpc.pb.h"

namespace dfs {
namespace grpc_client {

class MasterMetadataServiceClient {
   public:
    MasterMetadataServiceClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(protos::grpc::MasterMetadataService::NewStub(channel)) {}

    google::protobuf::util::StatusOr<protos::grpc::OpenFileRespond> SendRequest(
        const protos::grpc::OpenFileRequest& request);

    google::protobuf::util::Status SendRequest(
        const protos::grpc::DeleteFileRequest& request);

   private:
    std::unique_ptr<protos::grpc::MasterMetadataService::Stub> stub_;
};

}  // namespace grpc_client
}  // namespace dfs

#endif  // DFS_GRPC_CLIENT_MASTER_METADATA_SERVICE_CLIENT_H