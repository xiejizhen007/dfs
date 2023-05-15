#ifndef DFS_SERVER_CHUNK_SERVER_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_IMPL_H

#include <string>

#include "src/server/chunk_server/file_chunk_manager.h"

namespace dfs {
namespace server {

class ChunkServerImpl {
   public:
    bool Initialize();

   private:
    ChunkServerImpl() = default;

    ~ChunkServerImpl();

    std::string chunk_server_name_;

    FileChunkManager* file_chunk_manager_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_IMPL_H