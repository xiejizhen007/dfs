#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include "src/common/config_manager.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/server/chunk_server/chunk_cache_manager.h"
#include "src/server/chunk_server/chunk_server_impl.h"

namespace dfs {
namespace server {

using dfs::common::ConfigManager;
using dfs::common::StatusProtobuf2Grpc;
using google::protobuf::util::IsNotFound;
using protos::grpc::AdjustFileChunkVersionRespond;
using protos::grpc::ApplyMutationRequest;
using protos::grpc::ApplyMutationRespond;
using protos::grpc::FileChunkMutationStatus;
using protos::grpc::SendChunkDataRespond;
using protos::grpc::WriteFileChunkRespond;

FileChunkManager* ChunkServerFileServiceImpl::file_chunk_manager() {
    return FileChunkManager::GetInstance();
}

grpc::Status ChunkServerFileServiceImpl::InitFileChunk(
    grpc::ServerContext* context,
    const protos::grpc::InitFileChunkRequest* request,
    protos::grpc::InitFileChunkRespond* respond) {
    // get requested parameters
    const std::string& chunk_handle = request->chunk_handle();

    // add log
    LOG(INFO) << "create a chunk for chunk handle: " << chunk_handle;

    auto status = file_chunk_manager()->CreateChunk(chunk_handle, 1);
    if (status.ok()) {
        // successfully created
        respond->set_status(protos::grpc::InitFileChunkRespond::CREATED);
        return grpc::Status::OK;
    } else if (google::protobuf::util::IsAlreadyExists(status)) {
        // already exist
        respond->set_status(protos::grpc::InitFileChunkRespond::ALREADY_EXISTS);
        return grpc::Status::OK;
    } else {
        // internal error
        return dfs::common::StatusProtobuf2Grpc(status);
    }
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
    LOG(INFO) << "try to read chunk, chunk_handle: " << chunk_handle
              << ",version: " << version << ",offset: " << offset
              << ",length: " << length;

    auto read_status_or = file_chunk_manager()->ReadFromChunk(
        chunk_handle, version, offset, length);
    if (!read_status_or.ok()) {
        if (google::protobuf::util::IsNotFound(read_status_or.status())) {
            // 检查是不是版本不一致导致的
            auto version_status_ok =
                file_chunk_manager()->GetChunkVersion(chunk_handle);
            if (version_status_ok.ok()) {
                if (version_status_ok.value() != version) {
                    // 版本不一致导致读不到
                    respond->set_status(
                        protos::grpc::ReadFileChunkRespond::VERSION_ERROR);
                } else {
                    // 能到这吗？
                    return dfs::common::StatusProtobuf2Grpc(
                        version_status_ok.status());
                }
            } else if (google::protobuf::util::IsNotFound(
                           version_status_ok.status())) {
                // 压根没有对应的 chunk
                respond->set_status(
                    protos::grpc::ReadFileChunkRespond::NOT_FOUND);
            } else {
                // internal error
                return dfs::common::StatusProtobuf2Grpc(
                    version_status_ok.status());
            }
        } else if (google::protobuf::util::IsOutOfRange(
                       read_status_or.status())) {
            respond->set_status(
                protos::grpc::ReadFileChunkRespond::OUT_OF_RANGE);
        } else {
            return dfs::common::StatusProtobuf2Grpc(read_status_or.status());
        }

        // 对于 not found, version error, out of range 这三类错误，在 respond
        // 已经设置了错误码，所以只需要返回 ok，表明当前 rpc 操作正常
        // 如果是其他类型的错误，返回错误状态
        return grpc::Status::OK;
    }

    const auto& read_data = read_status_or.value();
    // LOG(INFO) << "read_data: " << read_data << "  "
    //           << "length: " << read_data.size();

    LOG(INFO) << "data length: " << read_data.size();

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

    auto status = WriteFileChunkLocally(header, respond);
    // write failed
    if (respond->status() != protos::grpc::FileChunkMutationStatus::OK) {
        return status;
    }

    // write successful
    // TODO: apply changes to other chunk server
    for (auto location : request->locations()) {
        const std::string server_address =
            location.server_hostname() + ":" +
            std::to_string(location.server_port());
        auto client =
            ChunkServerImpl::GetInstance()
                ->GetOrCreateChunkServerFileServerClient(server_address);
        if (!client) {
            LOG(ERROR)
                << "can not get or create file service client, server_address: "
                << server_address;
            continue;
        }

        ApplyMutationRequest apply_mutation_request;
        *apply_mutation_request.mutable_headers() = request->header();
        auto apply_mutation_respond =
            client->SendRequest(apply_mutation_request);
        // TODO: use apply_mutation_respond
    }

    return grpc::Status::OK;
}

grpc::Status ChunkServerFileServiceImpl::SendChunkData(
    grpc::ServerContext* context,
    const protos::grpc::SendChunkDataRequest* request,
    protos::grpc::SendChunkDataRespond* respond) {
    *respond->mutable_request() = *request;
    // data too big
    if (request->data().size() >
        ConfigManager::GetInstance()->GetBlockSize() * dfs::common::bytesMB) {
        LOG(ERROR) << "send chunk data is too big";
        respond->set_status(SendChunkDataRespond::DATA_TOO_BIG);
        return grpc::Status::OK;
    }

    // 对比校验和
    auto checksum = dfs::common::ComputeHash(request->data());
    if (checksum != request->checksum()) {
        LOG(ERROR) << "send chunk data checksum failed";
        respond->set_status(SendChunkDataRespond::BAD_DATA);
        return grpc::Status::OK;
    }

    LOG(INFO) << "caching data";
    ChunkCacheManager::GetInstance()->Set(request->checksum(), request->data());
    respond->set_status(SendChunkDataRespond::OK);

    return grpc::Status::OK;
}

grpc::Status ChunkServerFileServiceImpl::WriteFileChunkLocally(
    const protos::grpc::WriteFileChunkRequestHeader& header,
    protos::grpc::WriteFileChunkRespond* respond) {
    LOG(INFO) << "check data checksum: " << header.checksum()
              << " chunk handle: " << header.chunk_handle();
    // get data from cache
    auto data_or = ChunkCacheManager::GetInstance()->Get(header.checksum());
    if (!data_or.ok()) {
        LOG(ERROR) << "data not found in cache for checksum: "
                   << header.checksum();
        respond->set_status(FileChunkMutationStatus::NOT_FOUND);
        return grpc::Status::OK;
    }

    // 将 cache 里读到的数据写入
    auto write_result = file_chunk_manager()->WriteToChunk(
        header.chunk_handle(), header.version(), header.offset(),
        header.length(), data_or.value());

    LOG(INFO) << "write to local status: " + write_result.status().ToString();

    if (write_result.ok()) {
        respond->set_write_length(write_result.value());
        respond->set_status(protos::grpc::FileChunkMutationStatus::OK);
        return grpc::Status::OK;
    } else if (google::protobuf::util::IsNotFound(write_result.status())) {
        // TODO: 版本问题导致无法写入？
        respond->set_status(protos::grpc::FileChunkMutationStatus::NOT_FOUND);
        return grpc::Status::OK;
    } else if (google::protobuf::util::IsOutOfRange(write_result.status())) {
        respond->set_status(
            protos::grpc::FileChunkMutationStatus::OUT_OF_RANGE);
        return grpc::Status::OK;
    } else {
        return dfs::common::StatusProtobuf2Grpc(write_result.status());
    }
}

grpc::Status ChunkServerFileServiceImpl::ApplyMutation(
    grpc::ServerContext* context,
    const protos::grpc::ApplyMutationRequest* request,
    protos::grpc::ApplyMutationRespond* respond) {
    LOG(INFO) << "ApplyMutationRequest";
    // 应用对数据块的更改
    WriteFileChunkRespond write_respond;
    // 将数据写入本地
    auto status = WriteFileChunkLocally(request->headers(), &write_respond);
    respond->set_status(write_respond.status());
    return status;
}

grpc::Status ChunkServerFileServiceImpl::AdjustFileChunkVersion(
    grpc::ServerContext* context,
    const protos::grpc::AdjustFileChunkVersionRequest* request,
    protos::grpc::AdjustFileChunkVersionRespond* respond) {
    LOG(INFO) << "AdjustFileChunkVersionRequest";
    // 正常来说，在调整数据块版本时，我们只对版本号 +1
    const uint32_t old_version = request->new_chunk_version() - 1;
    auto update_version = file_chunk_manager()->UpdateChunkVersion(
        request->chunk_handle(), old_version, request->new_chunk_version());
    if (update_version.ok()) {
        LOG(INFO) << "update file chunk " << request->chunk_handle()
                  << " to version " << request->new_chunk_version();
        respond->set_status(AdjustFileChunkVersionRespond::OK);
        respond->set_chunk_version(request->new_chunk_version());
        return grpc::Status::OK;
    } else if (IsNotFound(update_version)) {
        // 是不是因为版本导致找不到数据块？
        auto version_or =
            file_chunk_manager()->GetChunkVersion(request->chunk_handle());
        if (version_or.ok()) {
            // 数据块存在，说明是因为数据块版本不同步导致的数据写入错误
            LOG(ERROR) << "file chunk " << request->chunk_handle()
                       << " version is not sync, chunk server has version "
                       << version_or.value()
                       << " but request try to update version from "
                       << old_version << " to " << request->new_chunk_version();
            respond->set_status(
                AdjustFileChunkVersionRespond::FAILED_VERSION_NOT_SYNC);
            return grpc::Status::OK;
        } else {
            // 数据块不存在？
            if (IsNotFound(version_or.status())) {
                LOG(ERROR) << "try to update file chunk "
                           << request->chunk_handle()
                           << " but chunk does not exist";
                respond->set_status(
                    AdjustFileChunkVersionRespond::FAILED_NOT_FOUND);
                return grpc::Status::OK;
            } else {
                // 其他错误
                LOG(ERROR) << "Unknow error in AdjustFileChunkVersion, status: "
                           << version_or.status().ToString();
                return StatusProtobuf2Grpc(version_or.status());
            }
        }
    } else {
        // 其他类型的错误
        LOG(ERROR) << "Unknow error in AdjustFileChunkVersion, status: "
                   << update_version.ToString();
        return StatusProtobuf2Grpc(update_version);
    }
}

}  // namespace server
}  // namespace dfs