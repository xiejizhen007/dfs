#include "src/server/chunk_server/chunk_cache_manager.h"

namespace dfs {
namespace server {

using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;

ChunkCacheManager* ChunkCacheManager::GetInstance() {
    static ChunkCacheManager* instance = new ChunkCacheManager();
    return instance;
}

google::protobuf::util::StatusOr<std::string> ChunkCacheManager::Get(
    const std::string& key) {
    auto value_pair = cache_.TryGet(key);
    if (!value_pair.second) {
        return NotFoundError("key nou found: " + key);
    }

    return value_pair.first;
}

google::protobuf::util::Status ChunkCacheManager::Set(
    const std::string& key, const std::string& value) {
    cache_.Set(key, value);
    return OkStatus();
}

google::protobuf::util::Status ChunkCacheManager::Remove(
    const std::string& key) {
    cache_.Erase(key);
    return OkStatus();
}

}  // namespace server
}  // namespace dfs