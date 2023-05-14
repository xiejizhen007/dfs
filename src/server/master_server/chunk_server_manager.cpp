#include "src/server/master_server/chunk_server_manager.h"

namespace dfs {
namespace server {

ChunkServerManager* ChunkServerManager::GetInstance() {
    static ChunkServerManager* instance = new ChunkServerManager();
    return instance;
}

bool ChunkServerManager::RegisterChunkServer(
    std::shared_ptr<protos::ChunkServer> chunk_server) {
    chunk_servers_.Set(chunk_server->location(), chunk_server);
    return true;
}

bool ChunkServerManager::UnRegisterChunkServer(
    const protos::ChunkServerLocation& location) {
    chunk_servers_.Erase(location);
    return true;
}

std::shared_ptr<protos::ChunkServer> ChunkServerManager::GetChunkServer(
    const protos::ChunkServerLocation& location) {
    auto value_pair = chunk_servers_.TryGet(location);
    return value_pair.first;
}

}  // namespace server
}  // namespace dfs