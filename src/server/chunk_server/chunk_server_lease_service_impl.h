#ifndef DFS_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H

#include "chunk_server_lease_service.grpc.pb.h"

namespace dfs {
namespace server {

class ChunkServerLeaseServiceImpl final
    : public protos::grpc::ChunkServerLeaseService::Service {
   public:
    grpc::Status GrantLease(grpc::ServerContext* context,
                            const protos::grpc::GrantLeaseRequest* request,
                            protos::grpc::GrantLeaseRespond* respond) override;

    grpc::Status RevokeLease(
        grpc::ServerContext* context,
        const protos::grpc::RevokeLeaseRequest* request,
        protos::grpc::RevokeLeaseRespond* respond) override;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_LEASE_SERVICE_IMPL_H