#include "src/server/chunk_server/chunk_server_impl.h"

namespace dfs {
namespace server {

ChunkServerImpl::~ChunkServerImpl() {

}

bool ChunkServerImpl::Initialize() {
    file_chunk_manager_ = FileChunkManager::GetInstance();
    return true;
}

}
}