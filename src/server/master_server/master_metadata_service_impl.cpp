#include "src/server/master_server/master_metadata_service_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace dfs {
namespace server {

using dfs::common::StatusProtobuf2Grpc;
using dfs::grpc_client::ChunkServerFileServiceClient;
using protos::grpc::InitFileChunkRequest;

MasterMetadataServiceImpl::MasterMetadataServiceImpl() {}

ChunkServerManager* MasterMetadataServiceImpl::chunk_server_manager() {
    return ChunkServerManager::GetInstance();
}

MetadataManager* MasterMetadataServiceImpl::metadata_manager() {
    return MetadataManager::GetInstance();
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    const std::string filename = request->filename();
    LOG(INFO) << "MasterMetadataServiceImpl::HandleFileCreation, filename: "
              << filename;

    // 检查是不是已经有该文件了
    if (metadata_manager()->ExistFileMetadata(filename)) {
        LOG(ERROR) << "File creation is failed, because file already exist: "
                   << filename;
        return grpc::Status(grpc::ALREADY_EXISTS, "file already exist");
    }

    //
    auto status = metadata_manager()->CreateFileMetadata(filename);
    if (!status.ok()) {
        LOG(ERROR) << "can't create file metadata, err msg: "
                   << status.message();
        return dfs::common::StatusProtobuf2Grpc(status);
    }

    //
    auto chunk_creation_status = HandleFileChunkCreation(request, respond);
    if (!chunk_creation_status.ok()) {
        LOG(ERROR) << "handle file chunk creation failed, rollback, clearing "
                      "failed metadata";
        metadata_manager()->DeleteFileAndChunkMetadata(filename);
    }

    return chunk_creation_status;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkRead(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    // get the filename, chunk_index
    const std::string& filename = request->filename();
    const uint32_t chunk_index = request->chunk_index();
    LOG(INFO) << "Handle file read: " << filename
              << " chunk idx: " << chunk_index;

    //
    if (!metadata_manager()->ExistFileMetadata(filename)) {
        LOG(ERROR) << "HandleFileChunkRead: can't read file, no exist "
                   << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file does not exists ");
    }

    // get the chunk handle, uuid
    auto chunk_handle_or =
        metadata_manager()->GetChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "HandleFileChunkRead: can't get chunk handle";
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    }

    const auto& chunk_handle = chunk_handle_or.value();
    auto file_chunk_metadata_or =
        metadata_manager()->GetFileChunkMetadata(chunk_handle);
    if (!file_chunk_metadata_or.ok()) {
        LOG(ERROR) << "";
        return dfs::common::StatusProtobuf2Grpc(
            file_chunk_metadata_or.status());
    }

    protos::FileChunkMetadata respondMetadata = file_chunk_metadata_or.value();
    respond->mutable_metadata()->set_chunk_handle(chunk_handle);
    respond->mutable_metadata()->set_version(respondMetadata.version());

    // TODO set up chunk server
    for (const auto& location :
         chunk_server_manager()->GetChunkLocation(chunk_handle)) {
        *respond->mutable_metadata()->add_locations() = location;
    }

    if (respond->metadata().locations().empty()) {
        LOG(ERROR) << "no chunk server to chunk handle: " << chunk_handle;
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "no chunk server is available");
    }

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkWrite(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    // get the filename, chunk_index
    const std::string& filename = request->filename();
    const uint32_t chunk_index = request->chunk_index();

    //
    if (!metadata_manager()->ExistFileMetadata(filename)) {
        LOG(ERROR) << "HandleFileChunkWrite: can't read file, no exist "
                   << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file does not exists ");
    }

    auto chunk_handle_or =
        metadata_manager()->GetChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "HandleFileChunkWrite: can't get chunk handle";
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    }

    // get the chunk handle
    const auto& chunk_handle = chunk_handle_or.value();
    auto file_chunk_metadata_or =
        metadata_manager()->GetFileChunkMetadata(chunk_handle);
    if (!file_chunk_metadata_or.ok()) {
        LOG(ERROR) << "";
        return dfs::common::StatusProtobuf2Grpc(
            file_chunk_metadata_or.status());
    }

    // get file chunk metadata
    protos::FileChunkMetadata respondMetadata = file_chunk_metadata_or.value();
    auto chunk_version = respondMetadata.version();
    auto new_chunk_version = chunk_version + 1;

    // TODO: set up chunk server
    for (const auto& location :
         chunk_server_manager()->GetChunkLocation(chunk_handle)) {
        const std::string server_address =
            location.server_hostname() + ":" +
            std::to_string(location.server_port());

        // auto client = GetChunkServerFileServiceClient(server_address);
    }

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkCreation(
    const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    const auto filename = request->filename();
    const auto chunk_index = request->chunk_index();

    if (!metadata_manager()->ExistFileMetadata(filename)) {
        LOG(ERROR) << "File metadata is not found, " << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file metadata is not found");
    }

    // create file chunk
    auto chunk_handle_or =
        metadata_manager()->CreateChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "chunk handle creation failed: "
                   << chunk_handle_or.status();
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    } else {
        LOG(INFO) << "chunk handle created: " << chunk_handle_or.value()
                  << " for file " << filename;
    }

    // get chunk_handle
    const auto& chunk_handle = chunk_handle_or.value();

    // TODO: assign chunk servers to store this chunk
    auto chunk_server_locations =
        chunk_server_manager()->AssignChunkServer(chunk_handle, 1);
    LOG(INFO) << "assign " << chunk_server_locations.size() << " chunk server";
    protos::FileChunkMetadata chunk_metadata;
    chunk_metadata.set_chunk_handle(chunk_handle);
    chunk_metadata.set_version(1);
    metadata_manager()->SetFileChunkMetadata(chunk_metadata);

    respond->mutable_metadata()->set_chunk_handle(chunk_handle);
    respond->mutable_metadata()->set_version(1);

    // TODO: talk to chunk server
    for (const auto& location :
         chunk_server_manager()->GetChunkLocation(chunk_handle)) {
        const std::string server_address =
            location.server_hostname() + ":" +
            std::to_string(location.server_port());

        auto client = GetChunkServerFileServiceClient(server_address);
        // set up request, and send request
        InitFileChunkRequest request;
        request.set_chunk_handle(chunk_handle);
        auto init_chunk_or = client->SendRequest(request);

        if (!init_chunk_or.ok()) {
            LOG(ERROR) << "failed init request";
            return StatusProtobuf2Grpc(init_chunk_or.status());
        }

        // ok
        *respond->mutable_metadata()->add_locations() = location;
    }

    if (respond->metadata().locations().empty()) {
        LOG(ERROR) << "no chunk server to chunk handle: " << chunk_handle;
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "no chunk server is available");
    }

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::OpenFile(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    switch (request->mode()) {
        case protos::grpc::OpenFileRequest::CREATE:
            return HandleFileCreation(request, respond);
        case protos::grpc::OpenFileRequest::READ:
            return HandleFileChunkRead(request, respond);
        case protos::grpc::OpenFileRequest::WRITE:
            return HandleFileChunkWrite(request, respond);
        default:
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "OpenFile got invaild mode");
    }
}

grpc::Status MasterMetadataServiceImpl::DeleteFile(
    grpc::ServerContext* context,
    const protos::grpc::DeleteFileRequest* request,
    google::protobuf::Empty* respond) {
    // get filename from request
    const std::string& filename = request->filename();
    LOG(INFO) << "Delete file: " << filename;
    metadata_manager()->DeleteFileAndChunkMetadata(filename);
    return grpc::Status::OK;
}

std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
MasterMetadataServiceImpl::GetChunkServerFileServiceClient(
    const std::string& server_address) {
    absl::WriterMutexLock chunk_server_file_service_clients_lock_guard(
        &chunk_server_file_service_clients_lock_);
    if (!chunk_server_file_service_clients_.contains(server_address)) {
        // create client
        chunk_server_file_service_clients_[server_address] =
            std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }

    return chunk_server_file_service_clients_[server_address];
}

}  // namespace server
}  // namespace dfs