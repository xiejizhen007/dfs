#include "src/server/master_server/chunk_server_manager.h"

namespace protos {
bool operator==(const ChunkServerLocation& l, const ChunkServerLocation& r) {
    return l.server_hostname() == r.server_hostname() &&
           l.server_port() == r.server_port();
}

bool operator==(const ChunkServer& l, const ChunkServer& r) {
    return l.location() == r.location();
}

}  // namespace protos

namespace dfs {
namespace server {

ChunkServerManager* ChunkServerManager::GetInstance() {
    static ChunkServerManager* instance = new ChunkServerManager();
    return instance;
}

bool ChunkServerManager::RegisterChunkServer(
    std::shared_ptr<protos::ChunkServer> chunk_server) {
    auto result =
        chunk_servers_.TryInsert(chunk_server->location(), chunk_server);
    if (result) {
        // handle store chunk
        for (int i = 0; i < chunk_server->stored_chunk_handles_size(); i++) {
            chunk_locations_[chunk_server->stored_chunk_handles()[i]].Insert(
                chunk_server->location());
        }
    }

    return result;
}

bool ChunkServerManager::UnRegisterChunkServer(
    const protos::ChunkServerLocation& location) {
    auto result = chunk_servers_.TryGet(location);

    // not found
    if (!result.second) {
        return false;
    }

    auto chunk_server = result.first;

    chunk_servers_.Erase(chunk_server->location());

    // 在 master 中删除掉 <chunk_handle, location> 映射，表明 chunk_handle
    // 所代表的 chunk 已不存在于 location 所代表的 chunkserver
    for (int i = 0; i < chunk_server->stored_chunk_handles_size(); i++) {
        chunk_locations_[chunk_server->stored_chunk_handles()[i]].Erase(
            chunk_server->location());
    }

    return true;
}

std::shared_ptr<protos::ChunkServer> ChunkServerManager::GetChunkServer(
    const protos::ChunkServerLocation& location) {
    auto value_pair = chunk_servers_.TryGet(location);
    return value_pair.first;
}

dfs::common::parallel_hash_set<protos::ChunkServerLocation,
                               ChunkServerLocationHash>
ChunkServerManager::GetChunkLocation(const std::string& chunk_handle) {
    return chunk_locations_[chunk_handle];
}

}  // namespace server
}  // namespace dfs