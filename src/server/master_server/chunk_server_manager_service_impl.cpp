#include "src/server/master_server/chunk_server_manager_service_impl.h"

#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

namespace dfs {
namespace server {

grpc::Status ChunkServerManagerServiceImpl::ReportChunkServer(
    grpc::ServerContext* context,
    const protos::grpc::ReportChunkServerRequest* request,
    protos::grpc::ReportChunkServerRespond* respond) {
    // 从 request 中获取 chunk_server 信息
    auto info = request->chunk_server();
    LOG(INFO) << "Master handle request from "
              << info.location().server_hostname() + ":" +
                     std::to_string(info.location().server_port());
    auto chunk_server =
        ChunkServerManager::GetInstance()->GetChunkServer(info.location());
    if (!chunk_server) {
        chunk_server = std::make_shared<protos::ChunkServer>(info);
        if (!ChunkServerManager::GetInstance()->RegisterChunkServer(
                chunk_server)) {
            LOG(ERROR) << "can not register chunk server";
            return grpc::Status(grpc::StatusCode::UNKNOWN,
                                "register chunk server failed");
        } else {
            LOG(INFO) << "register chunk server";
        }
        return grpc::Status::OK;
    }

    if (!chunk_server) {
        LOG(ERROR) << "wtf";
        return grpc::Status(grpc::StatusCode::UNKNOWN, "no chunk server");
    }

    // 需要新加到 master 里的
    absl::flat_hash_set<std::string> chunks_to_add;
    // 从 chunkserver 删掉
    absl::flat_hash_set<std::string> chunks_to_remove;

    for (const auto& chunk_handle :
         request->chunk_server().stored_chunk_handles()) {
        if (dfs::server::MetadataManager::GetInstance()->ExistFileChunkMetadata(
                chunk_handle)) {
            chunks_to_add.insert(chunk_handle);
        } else {
            // 当前 chunk 被 master 标记为删除，所以 chunkserver 之后可以删除他们
            *respond->add_delete_chunk_handles() = chunk_handle;
        }
    }

    for (const auto& chunk_handle : chunk_server->stored_chunk_handles()) {
        if (chunks_to_add.contains(chunk_handle)) {
            chunks_to_add.erase(chunk_handle);
        } else {
            chunks_to_remove.insert(chunk_handle);
        }
    }

    dfs::server::ChunkServerManager::GetInstance()->UpdateChunkServer(
        info.location(), info.available_disk_mb(), chunks_to_add,
        chunks_to_remove);

    return grpc::Status::OK;
}

}  // namespace server
}  // namespace dfs