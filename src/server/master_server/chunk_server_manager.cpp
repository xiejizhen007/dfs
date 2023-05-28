#include "src/server/master_server/chunk_server_manager.h"

#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_replica_manager.h"
#include "src/server/master_server/metadata_manager.h"

namespace protos {
bool operator==(const ChunkServerLocation& l, const ChunkServerLocation& r) {
    return l.server_hostname() == r.server_hostname() &&
           l.server_port() == r.server_port();
}

bool operator==(const ChunkServer& l, const ChunkServer& r) {
    return l.location() == r.location();
}

bool operator<(const ChunkServer& l, const ChunkServer& r) {
    return l.available_disk_mb() < r.available_disk_mb();
}

}  // namespace protos

namespace dfs {
namespace server {

using dfs::grpc_client::ChunkServerFileServiceClient;
using dfs::grpc_client::ChunkServerLeaseServiceClient;
using protos::ChunkServerLocation;

std::vector<protos::ChunkServerLocation> ChunkServerLocationFlatSetToVector(
    const ChunkServerLocationFlatSet& location_set) {
    std::vector<ChunkServerLocation> locations;

    for (const ChunkServerLocation& location : location_set) {
        locations.emplace_back(location);
    }

    return locations;
}

std::string ChunkServerLocationToString(const protos::ChunkServerLocation& location) {
    return location.server_hostname() + ":" + std::to_string(location.server_port());
}

ChunkServerManager* ChunkServerManager::GetInstance() {
    static ChunkServerManager* instance = new ChunkServerManager();
    return instance;
}

bool ChunkServerManager::RegisterChunkServer(
    std::shared_ptr<protos::ChunkServer> chunk_server) {
    absl::WriterMutexLock chunk_server_maps_lock_guard(
        &chunk_server_maps_lock_);
    absl::WriterMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);

    if (chunk_server_maps_.contains(chunk_server->location())) {
        return false;
    }

    chunk_server_maps_[chunk_server->location()] = chunk_server;
    // handle store chunk
    for (int i = 0; i < chunk_server->stored_chunk_handles_size(); i++) {
        chunk_location_maps_[chunk_server->stored_chunk_handles()[i]].insert(
            chunk_server->location());
    }

    return true;
}

bool ChunkServerManager::UnRegisterChunkServer(
    const protos::ChunkServerLocation& location) {
    absl::WriterMutexLock chunk_server_maps_lock_guard(
        &chunk_server_maps_lock_);
    absl::WriterMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);

    if (!chunk_server_maps_.contains(location)) {
        return false;
    }

    auto chunk_server = chunk_server_maps_[location];
    chunk_server_maps_.erase(chunk_server->location());

    // 在 master 中删除掉 <chunk_handle, location> 映射，表明 chunk_handle
    // 所代表的 chunk 已不存在于 location 所代表的 chunkserver
    for (int i = 0; i < chunk_server->stored_chunk_handles_size(); i++) {
        chunk_location_maps_[chunk_server->stored_chunk_handles()[i]].erase(
            chunk_server->location());

        // TODO: get a new primary server
        // 如果说被删除的服务器是主副本块服务器，需要寻找新的块服务器作为主副本块服务器
        UpdateFileChunkMetadataLocation(chunk_server->stored_chunk_handles()[i],
                                        chunk_server->location());

        // 复制副本
        ChunkReplicaManager::GetInstance()->AddChunkReplicaTask(
            chunk_server->stored_chunk_handles()[i]);
    }

    return true;
}

std::shared_ptr<protos::ChunkServer> ChunkServerManager::GetChunkServer(
    const protos::ChunkServerLocation& location) {
    absl::ReaderMutexLock chunk_server_maps_lock_guard(
        &chunk_server_maps_lock_);
    if (!chunk_server_maps_.contains(location)) {
        return nullptr;
    }
    return chunk_server_maps_[location];
}

ChunkServerLocationFlatSet ChunkServerManager::GetChunkLocation(
    const std::string& chunk_handle) {
    absl::ReaderMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);
    return chunk_location_maps_[chunk_handle];
}

size_t ChunkServerManager::GetChunkLocationSize(
    const std::string& chunk_handle) {
    absl::ReaderMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);
    return chunk_location_maps_[chunk_handle].size();
}

ChunkServerLocationFlatSet ChunkServerManager::GetChunkLocationNoLock(
    const std::string& chunk_handle) {
    return chunk_location_maps_[chunk_handle];
}

ChunkServerLocationFlatSet ChunkServerManager::AssignChunkServer(
    const std::string& chunk_handle, const uint32_t& server_request_nums) {
    absl::ReaderMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);

    if (chunk_location_maps_.contains(chunk_handle) &&
        chunk_location_maps_[chunk_handle].size() > 0) {
        return chunk_location_maps_[chunk_handle];
    }

    ChunkServerLocationFlatSet assigned_locations;
    int assigned_nums = 0;
    // TODO: 选择磁盘剩余空间最多的块服务器
    for (const auto& chunk_server : chunk_server_maps_) {
        const auto& location = chunk_server.first;
        if (assigned_nums >= (int)server_request_nums) {
            break;
        }

        assigned_locations.insert(location);
        chunk_location_maps_[chunk_handle].insert(location);
        assigned_nums++;
    }

    return assigned_locations;
}

ChunkServerLocationFlatSet ChunkServerManager::AssignChunkServerToCopyReplica(
    const std::string& chunk_handle, const uint32_t& healthy_replica_nums) {
    absl::ReaderMutexLock chunk_location_maps_lock_guard(
        &chunk_location_maps_lock_);
    ChunkServerLocationFlatSet assigned_locations;

    if (!chunk_location_maps_.contains(chunk_handle)) {
        LOG(ERROR) << "no chunk replica to copy";
        return assigned_locations;
    }

    if (chunk_location_maps_[chunk_handle].size() >= healthy_replica_nums) {
        LOG(INFO) << "have " << chunk_location_maps_[chunk_handle].size()
                  << " in server, no need copy";
        return assigned_locations;
    }

    int replica_num = chunk_location_maps_[chunk_handle].size();
    // TODO: 优先选择磁盘容量充足的块服务器
    for (const auto& location_pair : chunk_server_maps_) {
        if (replica_num >= healthy_replica_nums) {
            break;
        }

        if (!chunk_location_maps_[chunk_handle].contains(location_pair.first)) {
            assigned_locations.insert(location_pair.first);
            replica_num++;
        }
    }

    return assigned_locations;
}

void ChunkServerManager::UpdateChunkServer(
    const protos::ChunkServerLocation& location,
    const uint32_t& available_disk_mb,
    const absl::flat_hash_set<std::string>& chunks_to_add,
    const absl::flat_hash_set<std::string>& chunks_to_remove) {
    auto chunk_server = GetChunkServer(location);
    if (!chunk_server) {
        LOG(ERROR) << "no chunk server, " + location.server_hostname() + ":" +
                          std::to_string(location.server_port());
        return;
    }

    LOG(INFO) << "start to update chunk server";

    if (!chunks_to_remove.empty()) {
        for (auto iter = chunk_server->stored_chunk_handles().begin();
             iter != chunk_server->stored_chunk_handles().end();) {
            auto chunk_handle = *iter;

            if (chunks_to_remove.contains(chunk_handle)) {
                iter =
                    chunk_server->mutable_stored_chunk_handles()->erase(iter);
                chunk_location_maps_[chunk_handle].erase(
                    chunk_server->location());
            } else {
                ++iter;
            }
        }
    }

    for (const auto& chunk_handle : chunks_to_add) {
        chunk_server->add_stored_chunk_handles(chunk_handle);
        chunk_location_maps_[chunk_handle].insert(chunk_server->location());
    }

    chunk_server->set_available_disk_mb(available_disk_mb);
}

void ChunkServerManager::UpdateFileChunkMetadataLocation(
    const std::string& chunk_handle,
    const protos::ChunkServerLocation& location) {
    auto metadata_or =
        MetadataManager::GetInstance()->GetFileChunkMetadata(chunk_handle);
    if (!metadata_or.ok()) {
        LOG(ERROR) << "UpdateFileChunkMetadataLocation: get chunk handle "
                   << chunk_handle << " metadata failed, because "
                   << metadata_or.status();
        return;
    }

    auto metadata = metadata_or.value();
    if (metadata.primary_location() == location) {
        LOG(WARNING)
            << "UpdateFileChunkMetadataLocation: warning, chunk handle "
            << chunk_handle
            << " primary server is unregisted, need to get new primary server";

        // 分配一个新的块服务器作为主副本服务器
        auto locations = GetChunkLocationNoLock(chunk_handle);
        if (locations.empty()) {
            LOG(ERROR) << "wtf? no chunk server store chunk handle "
                       << chunk_handle;
            return;
        }

        for (const auto& primary_location : locations) {
            *metadata.mutable_primary_location() = primary_location;
            LOG(INFO) << "chunk handle " << chunk_handle
                      << " set new primary location, "
                      << primary_location.DebugString();
            break;
        }

        MetadataManager::GetInstance()->SetFileChunkMetadata(metadata);
    }
}

std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
ChunkServerManager::GetOrCreateChunkServerFileServiceClient(
    const std::string& server_address) {
    // 锁住
    absl::MutexLock chunk_server_file_service_clients_lock_guard(
        &chunk_server_file_service_clients_lock_);

    // 创建客户端
    if (!chunk_server_file_service_clients_.contains(server_address)) {
        chunk_server_file_service_clients_[server_address] =
            std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }

    // 客户端可能失效了？会不会出现这种情况
    if (!chunk_server_file_service_clients_[server_address]) {
        chunk_server_file_service_clients_[server_address] =
            std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }

    return chunk_server_file_service_clients_[server_address];
}

std::shared_ptr<dfs::grpc_client::ChunkServerLeaseServiceClient>
ChunkServerManager::GetOrCreateChunkServerLeaseServiceClient(
    const std::string& server_address) {
    // 锁住
    absl::MutexLock chunk_server_lease_service_clients_lock_guard(
        &chunk_server_lease_service_clients_lock_);

    // 创建客户端
    if (!chunk_server_lease_service_clients_.contains(server_address)) {
        chunk_server_lease_service_clients_[server_address] =
            std::make_shared<ChunkServerLeaseServiceClient>(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }

    // 客户端可能失效了？会不会出现这种情况
    if (!chunk_server_lease_service_clients_[server_address]) {
        chunk_server_lease_service_clients_[server_address] =
            std::make_shared<ChunkServerLeaseServiceClient>(grpc::CreateChannel(
                server_address, grpc::InsecureChannelCredentials()));
    }

    return chunk_server_lease_service_clients_[server_address];
}

}  // namespace server
}  // namespace dfs