#include "src/client/dfs_client_impl.h"

namespace dfs {
namespace client {

using dfs::grpc_client::MasterMetadataServiceClient;
using google::protobuf::util::OkStatus;
using protos::grpc::OpenFileRequest;

DfsClientImpl::DfsClientImpl() {
    // TODO: read ip:port from config file.
    const std::string& target_str = "127.0.0.1:1234";
    auto channel =
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());
    master_metadata_service_client_ =
        std::make_shared<MasterMetadataServiceClient>(channel);
}

google::protobuf::util::Status DfsClientImpl::CreateFile(
    const std::string& filename) {
    // set up request
    OpenFileRequest request;
    request.set_filename(filename);
    request.set_mode(OpenFileRequest::CREATE);
    // call rpc, create file.
    auto respond_or = master_metadata_service_client_->SendRequest(request);
    if (!respond_or.ok()) {
        return respond_or.status();
    }
    // cache file metadata.
    return OkStatus();
}

google::protobuf::util::Status DfsClientImpl::DeleteFile(
    const std::string& filename) {
    return OkStatus();
}

}  // namespace client
}  // namespace dfs