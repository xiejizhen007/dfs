#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/server/chunk_server/chunk_server_impl.h"

namespace dfs {
namespace server {

using dfs::common::StatusProtobuf2Grpc;
using google::protobuf::util::IsNotFound;
using protos::grpc::GrantLeaseRespond;

grpc::Status ChunkServerLeaseServiceImpl::GrantLease(
    grpc::ServerContext* context,
    const protos::grpc::GrantLeaseRequest* request,
    protos::grpc::GrantLeaseRespond* respond) {
    // 从请求中获取相应信息
    const auto& chunk_handle = request->chunk_handle();
    const auto& chunk_version = request->chunk_version();
    LOG(INFO) << "GrantLease for " << chunk_handle;
    auto owned_version_or =
        ChunkServerImpl::GetInstance()->GetChunkVersion(chunk_handle);
    if (!owned_version_or.ok()) {
        if (IsNotFound(owned_version_or.status())) {
            LOG(INFO) << "can not accept lease " << chunk_handle
                      << " done't exist on this chunk server";
            respond->set_status(GrantLeaseRespond::REJECTED_NOT_FOUND);
            return grpc::Status::OK;
        } else {
            LOG(ERROR) << "can not accept lease " << chunk_handle << " because "
                       << owned_version_or.status().ToString();
            respond->set_status(GrantLeaseRespond::UNKNOW);
            return StatusProtobuf2Grpc(owned_version_or.status());
        }
    } else if (chunk_version != owned_version_or.value()) {
        LOG(INFO) << "can not accept lease " << chunk_handle
                  << " because request version is " << chunk_version
                  << " but owned version is " << owned_version_or.value();
        respond->set_status(GrantLeaseRespond::REJECTED_VERSION);
        return grpc::Status::OK;
    } else if (absl::Now() > absl::FromUnixSeconds(
                                 request->lease_expiration_time().seconds())) {
        LOG(INFO) << "can not accept lease " << chunk_handle
                  << " because the given lease is already expired";
        respond->set_status(GrantLeaseRespond::REJECTED_EXPIRED);
        return grpc::Status::OK;
    } else {
        //
        LOG(INFO) << "accept lease for " << chunk_handle;
        ChunkServerImpl::GetInstance()->AddOrUpdateLease(
            chunk_handle, request->lease_expiration_time().seconds());
        respond->set_status(GrantLeaseRespond::ACCEPTED);
        return grpc::Status::OK;
    }
}

grpc::Status ChunkServerLeaseServiceImpl::RevokeLease(
    grpc::ServerContext* context,
    const protos::grpc::RevokeLeaseRequest* request,
    protos::grpc::RevokeLeaseRespond* respond) {
    return grpc::Status::OK;
}

}  // namespace server
}  // namespace dfs