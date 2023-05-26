#include "src/server/master_server/chunk_server_manager.h"

#include "src/common/system_logger.h"

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

using protos::ChunkServerLocation;

std::vector<protos::ChunkServerLocation> ChunkServerLocationFlatSetToVector(
    const ChunkServerLocationFlatSet& location_set) {
    std::vector<ChunkServerLocation> locations;

    for (const ChunkServerLocation& location : location_set) {
        locations.emplace_back(location);
    }

    return locations;
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

}  // namespace server
}  // namespace dfs