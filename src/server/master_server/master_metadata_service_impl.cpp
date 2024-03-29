#include "src/server/master_server/master_metadata_service_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace dfs {
namespace server {

using dfs::common::StatusProtobuf2Grpc;
using dfs::grpc_client::ChunkServerFileServiceClient;
using protos::grpc::AdjustFileChunkVersionRequest;
using protos::grpc::AdjustFileChunkVersionRespond;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::GrantLeaseRespond;
using protos::grpc::InitFileChunkRequest;

MasterMetadataServiceImpl::MasterMetadataServiceImpl() {
    chunk_server_manager_ = ChunkServerManager::GetInstance();
    metadata_manager_ = MetadataManager::GetInstance();
}

grpc::Status MasterMetadataServiceImpl::HandleFileCreation(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    const std::string filename = request->filename();
    LOG(INFO) << "MasterMetadataServiceImpl::HandleFileCreation, filename: "
              << filename;

    // 检查是不是已经有该文件了
    if (metadata_manager_->ExistFileMetadata(filename)) {
        LOG(ERROR) << "File creation is failed, because file already exist: "
                   << filename;
        return grpc::Status(grpc::ALREADY_EXISTS, "file already exist");
    }

    //
    auto status = metadata_manager_->CreateFileMetadata(filename);
    if (!status.ok()) {
        LOG(ERROR) << "can't create file metadata, err msg: "
                   << status.message();
        return dfs::common::StatusProtobuf2Grpc(status);
    }

    //
    auto chunk_creation_status =
        HandleFileChunkCreation(context, request, respond);
    if (!chunk_creation_status.ok()) {
        LOG(ERROR) << "handle file chunk creation failed, rollback, clearing "
                      "failed metadata";
        metadata_manager_->DeleteFileAndChunkMetadata(filename);
    }

    return chunk_creation_status;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkRead(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    // get the filename, chunk_index
    const std::string& filename = request->filename();
    const uint32_t chunk_index = request->chunk_index();
    LOG(INFO) << "Handle file read: " << filename
              << " chunk idx: " << chunk_index;

    //
    if (!metadata_manager_->ExistFileMetadata(filename)) {
        LOG(ERROR) << "HandleFileChunkRead: can't read file, no exist "
                   << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file does not exists ");
    }

    // get the chunk handle, uuid
    auto chunk_handle_or =
        metadata_manager_->GetChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "HandleFileChunkRead: can't get chunk handle";
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    }

    const auto& chunk_handle = chunk_handle_or.value();
    auto file_chunk_metadata_or =
        metadata_manager_->GetFileChunkMetadata(chunk_handle);
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
         chunk_server_manager_->GetChunkLocation(chunk_handle)) {
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
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    /**
     * 当客户端发起写入请求时，主服务器确保客户端持有写入租约的情况下
     * 更新主副本服务器上数据块的版本，随后调整主服务器的数据块版本
     * 将更新后的块元数据返回给客户端。
     *
     * 主副本服务器在客户端写入数据后，会去同步其他副本服务器的数据，调整版本，更新数据等。
     */

    auto start = std::chrono::high_resolution_clock::now();  // 记录开始时间
    // get the filename, chunk_index
    const std::string& filename = request->filename();
    const uint32_t chunk_index = request->chunk_index();
    LOG(INFO) << "Handle file write: " << filename
              << " chunk idx: " << chunk_index;

    // 确保文件已经被创建
    if (!metadata_manager_->ExistFileMetadata(filename) &&
        !request->create_if_not_exists()) {
        LOG(ERROR) << "HandleFileChunkWrite: can't read file, no exist "
                   << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file does not exists");
    }

    auto chunk_handle_or =
        metadata_manager_->GetChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        if (!request->create_if_not_exists()) {
            LOG(ERROR) << "HandleFileChunkWrite: can't get chunk handle";
            return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
        }

        LOG(INFO) << "will create a file chunk for " << filename
                  << " at chunk index " << chunk_index;
        auto chunk_create_status =
            HandleFileChunkCreation(context, request, respond);
        if (!chunk_create_status.ok()) {
            LOG(ERROR) << "create file chunk failed, because "
                       << chunk_create_status.error_message();
            return chunk_create_status;
        }

        LOG(INFO) << "Successfully create file chunk.";

        // 刷新 chunk handle
        chunk_handle_or =
            metadata_manager_->GetChunkHandle(filename, chunk_index);
    }

    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "what happend, status: "
                   << chunk_handle_or.status().ToString();
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    }

    const std::string& chunk_handle = chunk_handle_or.value();

    // 当前租约已分配
    bool lease_granted = false;

    auto file_chunk_metadata_or =
        metadata_manager_->GetFileChunkMetadata(chunk_handle);
    if (!file_chunk_metadata_or.ok()) {
        LOG(ERROR) << "no file chunk metadata for " << filename
                   << ",index: " << chunk_index << ",handle: " << chunk_handle;
        return dfs::common::StatusProtobuf2Grpc(
            file_chunk_metadata_or.status());
    }
    // get file chunk metadata
    protos::FileChunkMetadata file_chunk_metadata =
        file_chunk_metadata_or.value();
    const auto chunk_version = file_chunk_metadata.version();

    // try to get lease
    auto lease_result_pair = metadata_manager_->GetLeaseMetadata(chunk_handle);
    if (lease_result_pair.second) {
        // 说明存在 chunk_handle 对应的租约
        if (absl::FromUnixSeconds(lease_result_pair.first.second) >
            absl::Now()) {
            if (context->peer() == lease_result_pair.first.first) {
                // 当前租约仍有效
                LOG(INFO) << "lease " << chunk_handle << " is ok";
                lease_granted = true;
            } else {
                // 被其他客户端拿到租约了
                LOG(INFO) << "other guy get the lease";
                return grpc::Status(grpc::StatusCode::UNKNOWN,
                                    "other guy get the lease");
            }
        } else {
            // 租约过期了
            LOG(INFO) << "lease " << chunk_handle << " is expired";
            metadata_manager_->RemoveLeaseMetadata(chunk_handle);
        }
    }

    if (!lease_granted) {
        // 需要分配新的租约
        const std::string primary_server_address =
            std::move(ChunkServerLocationToString(
                file_chunk_metadata.primary_location()));

        if (primary_server_address.size() < 3) {
            LOG(ERROR) << "get chunk metadata primary location error";
            return grpc::Status(grpc::StatusCode::UNKNOWN,
                                "get chunk metadata primary location error");
        }

        auto lease_client =
            chunk_server_manager_->GetOrCreateChunkServerLeaseServiceClient(
                primary_server_address);
        if (!lease_client) {
            LOG(ERROR) << "can not talk to " << primary_server_address
                       << " lease service";
            return grpc::Status(grpc::StatusCode::UNKNOWN,
                                "can not talk to lease service");
        }

        LOG(INFO) << "try to talk to primary server: "
                  << primary_server_address;

        // ok
        GrantLeaseRequest lease_req;
        lease_req.set_chunk_handle(chunk_handle);
        lease_req.set_chunk_version(chunk_version);
        // TODO: use config
        uint64_t next_expire_time =
            absl::ToUnixSeconds(absl::Now() + absl::Seconds(60));
        lease_req.mutable_lease_expiration_time()->set_seconds(
            next_expire_time);

        auto lease_respond = lease_client->SendRequest(lease_req);
        if (lease_respond.ok()) {
            if (lease_respond.value().status() == GrantLeaseRespond::ACCEPTED) {
                LOG(INFO) << "get lease ok, status: "
                          << lease_respond.value().status();
                metadata_manager_->SetLeaseMetadata(
                    chunk_handle, context->peer(), next_expire_time);
                lease_granted = true;
            } else {
                LOG(ERROR) << "can not get lease from primary server, because "
                              "not ok, status: "
                           << lease_respond.value().status();
            }
        } else {
            LOG(ERROR) << "can not get lease from primary server, because "
                       << lease_respond.status().ToString();
        }
    }

    if (!lease_granted) {
        // 这说明上面租约分配有问题
        // 没有租约，禁止写入数据
        LOG(ERROR) << "can not grant lease for " << chunk_handle;
        return grpc::Status(grpc::StatusCode::UNKNOWN,
                            "can not grant lease for " + chunk_handle);
    }

    // 调整版本
    {
        // talk to chunk server
        const std::string primary_server_address =
            std::move(ChunkServerLocationToString(
                file_chunk_metadata.primary_location()));

        auto primary_client =
            chunk_server_manager_->GetOrCreateChunkServerFileServiceClient(
                primary_server_address);

        AdjustFileChunkVersionRequest version_req;
        version_req.set_chunk_handle(chunk_handle);
        version_req.set_new_chunk_version(chunk_version + 1);
        auto version_respond_or = primary_client->SendRequest(version_req);
        if (!version_respond_or.ok()) {
            LOG(ERROR) << "can not adjust primary server chunk version";
            return StatusProtobuf2Grpc(version_respond_or.status());
        }

        // 确保主副本服务器调整版本完成后，调整主服务器的块版本
        auto inc_version_status =
            metadata_manager_->IncFileChunkVersion(chunk_handle);
        if (inc_version_status.ok()) {
            LOG(INFO) << "inc chunk " << chunk_handle << " version, from "
                      << chunk_version << "to " << chunk_version + 1;
        } else {
            LOG(ERROR) << "error in inc chunk version";
            return StatusProtobuf2Grpc(inc_version_status);
        }
    }

    // get the chunk handle
    LOG(INFO) << "wirte: try to get chunk handle " << chunk_handle
              << " metadata";
    auto new_chunk_version = chunk_version + 1;
    LOG(INFO) << "respond metadata chunk_handle: "
              << file_chunk_metadata.chunk_handle();

    respond->mutable_metadata()->set_chunk_handle(
        file_chunk_metadata.chunk_handle());
    respond->mutable_metadata()->set_version(new_chunk_version);
    *respond->mutable_metadata()->mutable_primary_location() =
        file_chunk_metadata.primary_location();
    for (auto location :
         chunk_server_manager_->GetChunkLocation(chunk_handle)) {
        *respond->mutable_metadata()->add_locations() = location;
    }

    auto end = std::chrono::high_resolution_clock::now();  // 记录结束时间
    double durationMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();
    LOG(INFO) << "HandleFileChunkWrite: " << durationMs << " ms";

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::HandleFileChunkCreation(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    const auto filename = request->filename();
    const auto chunk_index = request->chunk_index();

    LOG(INFO) << "HandleFileChunkCreation will create a chunk for " << filename
              << " idx: " << chunk_index;

    if (!metadata_manager_->ExistFileMetadata(filename)) {
        LOG(ERROR) << "File metadata is not found, " << filename;
        return grpc::Status(grpc::StatusCode::NOT_FOUND,
                            "file metadata is not found");
    }

    // create file chunk
    auto chunk_handle_or =
        metadata_manager_->CreateChunkHandle(filename, chunk_index);
    if (!chunk_handle_or.ok()) {
        LOG(ERROR) << "chunk handle creation failed: "
                   << chunk_handle_or.status();
        return dfs::common::StatusProtobuf2Grpc(chunk_handle_or.status());
    } else {
        LOG(INFO) << "chunk handle created: " << chunk_handle_or.value()
                  << " for file " << filename << " idx: " << chunk_index;
        auto file_metadata_or = metadata_manager_->GetFileMetadata(filename);
        LOG(INFO) << "file metadata contains idx: " << chunk_index << "? "
                  << file_metadata_or.value()->chunk_handles().contains(
                         chunk_index);
    }

    // get chunk_handle
    const auto& chunk_handle = chunk_handle_or.value();

    // TODO: assign chunk servers to store this chunk
    auto chunk_server_locations = ChunkServerLocationFlatSetToVector(
        chunk_server_manager_->AssignChunkServer(chunk_handle, 3));

    LOG(INFO) << "assign " << chunk_server_locations.size()
              << " chunk server to store chunk handle " << chunk_handle;
    // 当前无可用的块服务器了
    if (chunk_server_locations.empty()) {
        LOG(ERROR) << "no chunk server to store chunk handle: " << chunk_handle;
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "no chunk server is available");
    }

    for (auto location : chunk_server_locations) {
        LOG(INFO) << "debug location: " << location.DebugString();
    }

    protos::FileChunkMetadata chunk_metadata;
    chunk_metadata.set_chunk_handle(chunk_handle);
    chunk_metadata.set_version(1);
    // 选取当前磁盘剩余空间最多的块服务器作为主副本服务器
    // 只有当前的主副本服务器出现故障时，才会更换主副本服务器
    *chunk_metadata.mutable_primary_location() = chunk_server_locations[0];
    metadata_manager_->SetFileChunkMetadata(chunk_metadata);

    respond->mutable_metadata()->set_chunk_handle(chunk_handle);
    respond->mutable_metadata()->set_version(1);
    *respond->mutable_metadata()->mutable_primary_location() =
        chunk_server_locations[0];

    // TODO: talk to chunk server
    for (const auto& location :
         chunk_server_manager_->GetChunkLocation(chunk_handle)) {
        // 设置好块服务器地址
        const std::string server_address =
            std::move(ChunkServerLocationToString(location));

        auto client =
            chunk_server_manager_->GetOrCreateChunkServerFileServiceClient(
                server_address);
        // set up request, and send request
        InitFileChunkRequest request;
        request.set_chunk_handle(chunk_handle);
        auto init_chunk_or = client->SendRequest(request);

        if (!init_chunk_or.ok()) {
            LOG(ERROR) << "failed init request";
            return StatusProtobuf2Grpc(init_chunk_or.status());
        } else {
            LOG(INFO) << "InitFileChunkRequest is ok";
        }

        // ok
        *respond->mutable_metadata()->add_locations() = location;
    }

    if (respond->metadata().locations().empty()) {
        LOG(ERROR) << "no chunk server to chunk handle: " << chunk_handle;
        // 没有块服务器存储该数据块，删掉数据块
        return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                            "no chunk server is available");
    }

    auto get_chunk_handle_or =
        metadata_manager_->GetChunkHandle(filename, chunk_index);
    if (!get_chunk_handle_or.ok()) {
        LOG(ERROR) << "can not get chunk handle after create chunk";
    } else {
        LOG(INFO) << "create chunk is ok, filename: " << filename
                  << " idx: " << chunk_index;
    }

    return grpc::Status::OK;
}

grpc::Status MasterMetadataServiceImpl::OpenFile(
    grpc::ServerContext* context, const protos::grpc::OpenFileRequest* request,
    protos::grpc::OpenFileRespond* respond) {
    LOG(INFO) << "OpenFile: client url " << context->peer();
    switch (request->mode()) {
        case protos::grpc::OpenFileRequest::CREATE:
            return HandleFileCreation(context, request, respond);
        case protos::grpc::OpenFileRequest::READ:
            return HandleFileChunkRead(context, request, respond);
        case protos::grpc::OpenFileRequest::WRITE:
            return HandleFileChunkWrite(context, request, respond);
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
    metadata_manager_->DeleteFileAndChunkMetadata(filename);
    return grpc::Status::OK;
}

}  // namespace server
}  // namespace dfs