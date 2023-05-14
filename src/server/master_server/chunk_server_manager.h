#ifndef DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H

#include <memory>

#include "chunk_server.pb.h"
#include "src/common/utils.h"

namespace dfs {
namespace server {

class ChunkServerManager {
   public:
    static ChunkServerManager* GetInstance();

    bool RegisterChunkServer(std::shared_ptr<protos::ChunkServer> chunk_server);

    bool UnRegisterChunkServer(const protos::ChunkServerLocation& location);

    std::shared_ptr<protos::ChunkServer> GetChunkServer(const protos::ChunkServerLocation& location);

   private:
    ChunkServerManager() = default;

    dfs::common::parallel_hash_map<protos::ChunkServerLocation,
                                   std::shared_ptr<protos::ChunkServer>>
        chunk_servers_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H