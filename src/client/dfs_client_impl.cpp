#include "src/client/dfs_client_impl.h"

#include "src/common/utils.h"

namespace dfs {
namespace client {

using dfs::grpc_client::MasterMetadataServiceClient;
using google::protobuf::util::OkStatus;
using protos::grpc::DeleteFileRequest;
using protos::grpc::OpenFileRequest;
using protos::grpc::ReadFileChunkRequest;

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
    CacheToCacheManager(filename, 0, respond_or.value());
    return OkStatus();
}

google::protobuf::util::Status DfsClientImpl::DeleteFile(
    const std::string& filename) {
    // set up request
    DeleteFileRequest request;
    request.set_filename(filename);
    return master_metadata_service_client_->SendRequest(request);
}

google::protobuf::util::StatusOr<std::pair<size_t, std::string>>
DfsClientImpl::ReadFile(const std::string& filename, size_t offset,
                        size_t nbytes) {
    // 一个 chunk 4MB
    // TODO: use config file
    const size_t chunk_size = 4 * common::bytesMB;

    size_t bytes_read = 0;
    size_t remain_bytes = nbytes;
    size_t start_offset = offset % chunk_size;

    return OkStatus();
}

google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
DfsClientImpl::ReadFileChunk(const std::string& filename, size_t chunk_index,
                             size_t offset, size_t nbytes) {
    // TODO: set up chunk_handle, chunk_version from chunk metadata.
    ReadFileChunkRequest request;
    request.set_offset(offset);
    request.set_length(nbytes);
    return OkStatus();
}

void DfsClientImpl::CacheToCacheManager(
    const std::string& filename, const uint32_t& chunk_index,
    const protos::grpc::OpenFileRespond& respond) {
    const std::string& chunk_handle = respond.metadata().chunk_handle();

    auto set_chunk_handle_status =
        cache_manager_->SetChunkHandle(filename, chunk_index, chunk_handle);
    if (!set_chunk_handle_status.ok()) {
        // add log
        return;
    }

    auto chunk_version_or = cache_manager_->GetChunkVersion(chunk_handle);
    auto respond_version = respond.metadata().version();
    if (!chunk_version_or.ok() || respond_version > chunk_version_or.value()) {
        // no cache or find a higher version
        cache_manager_->SetChunkVersion(chunk_handle, respond_version);
    } else {
        // add log
    }
}

}  // namespace client
}  // namespace dfs