#ifndef DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H

#include <memory>

#include "chunk_server.pb.h"
#include "src/common/utils.h"

namespace protos {
bool operator==(const ChunkServerLocation& l, const ChunkServerLocation& r);

bool operator==(const ChunkServer& l, const ChunkServer& r);
}  // namespace protos

namespace dfs {
namespace server {

class ChunkServerLocationHash {
   public:
    std::size_t operator()(const protos::ChunkServerLocation& obj) const {
        std::size_t h1 = std::hash<std::string>()(obj.server_hostname());
        std::size_t h2 = std::hash<uint32_t>()(obj.server_port());
        return h1 ^ (h2 << 1);
    }
};

class ChunkServerManager {
   public:
    static ChunkServerManager* GetInstance();

    bool RegisterChunkServer(std::shared_ptr<protos::ChunkServer> chunk_server);

    bool UnRegisterChunkServer(const protos::ChunkServerLocation& location);

    std::shared_ptr<protos::ChunkServer> GetChunkServer(
        const protos::ChunkServerLocation& location);

    dfs::common::parallel_hash_set<protos::ChunkServerLocation,
                                   ChunkServerLocationHash>
    GetChunkLocation(const std::string& chunk_handle);

   private:
    ChunkServerManager() = default;

    //
    dfs::common::parallel_hash_map<protos::ChunkServerLocation,
                                   std::shared_ptr<protos::ChunkServer>,
                                   ChunkServerLocationHash>
        chunk_servers_;

    // store chunk location in memory
    dfs::common::parallel_hash_map<
        std::string, dfs::common::parallel_hash_set<protos::ChunkServerLocation,
                                                    ChunkServerLocationHash>>
        chunk_locations_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H