#include "src/server/master_server/master_metadata_service_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace dfs {
namespace server {

MasterMetadataServiceImpl::MasterMetadataServiceImpl() {}

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

    const auto& chunk_handle = chunk_handle_or.value();
    protos::FileChunkMetadata chunk_metadata;
    chunk_metadata.set_chunk_handle(chunk_handle);
    // 新的 chunk 版本都为 1，当 chunk 变化时进行累加
    chunk_metadata.set_version(1);
    metadata_manager()->SetFileChunkMetadata(chunk_metadata);

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::OpenFile(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    switch (request->mode()) {
        case protos::grpc::OpenFileRequest::CREATE:
            return HandleFileCreation(request, respond);
        case protos::grpc::OpenFileRequest::READ:

        case protos::grpc::OpenFileRequest::WRITE:

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

}  // namespace server
}  // namespace dfs