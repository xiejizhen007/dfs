#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace dfs {
namespace server {

FileChunkManager* ChunkServerFileServiceImpl::file_chunk_manager() {
    return FileChunkManager::GetInstance();
}

grpc::Status ChunkServerFileServiceImpl::ReadFileChunk(
    grpc::ServerContext* context,
    const protos::grpc::ReadFileChunkRequest* request,
    protos::grpc::ReadFileChunkRespond* respond) {
    // get requested parameters
    const std::string& chunk_handle = request->chunk_handle();
    const uint32_t& version = request->version();
    const uint32_t& offset = request->offset();
    const uint32_t& length = request->length();

    //
    LOG(INFO) << "try to read chunk, uuid: " + chunk_handle
              << "version: " + version << "offset: " + offset
              << "length: " + length;

    auto read_status_or = file_chunk_manager()->ReadFromChunk(
        chunk_handle, version, offset, length);
    if (!read_status_or.ok()) {
        LOG(ERROR) << "fail to read chunk, status: " +
                          read_status_or.status().ToString();
        if (google::protobuf::util::IsNotFound(read_status_or.status())) {
            respond->set_status(protos::grpc::ReadFileChunkRespond::NOT_FOUND);
        }
        return dfs::common::StatusProtobuf2Grpc(read_status_or.status());
    }

    const auto& read_data = read_status_or.value();

    respond->set_data(read_data);
    respond->set_read_length(read_data.size());
    respond->set_status(protos::grpc::ReadFileChunkRespond::OK);

    return grpc::Status::OK;
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunk(
    grpc::ServerContext* context,
    const protos::grpc::WriteFileChunkRequest* request,
    protos::grpc::WriteFileChunkRespond* respond) {
    // get requested parameters
    const auto& header = request->header();
    const auto& chunk_handle = header.chunk_handle();
    const auto& version = header.version();
    const auto& offset = header.offset();
    const auto& length = header.length();
    const auto& data = header.data();

    auto write_status_or = file_chunk_manager()->WriteToChunk(
        chunk_handle, version, offset, length, data);

    if (!write_status_or.ok()) {
        return dfs::common::StatusProtobuf2Grpc(write_status_or.status());
    }

    respond->set_write_length(write_status_or.value());

    return grpc::Status::OK;
}

}  // namespace server
}  // namespace dfs