#ifndef DFS_SERVER_CHUNK_CACHE_MANAGER_H
#define DFS_SERVER_CHUNK_CACHE_MANAGER_H

#include <google/protobuf/stubs/statusor.h>

#include "src/common/utils.h"

namespace dfs {
namespace server {

class ChunkCacheManager {
   public:
    static ChunkCacheManager* GetInstance();

    google::protobuf::util::StatusOr<std::string> Get(const std::string& key);

    google::protobuf::util::Status Set(const std::string& key,
                                       const std::string& value);

    google::protobuf::util::Status Remove(const std::string& key);

   private:
    ChunkCacheManager() = default;
    dfs::common::parallel_hash_map<std::string, std::string> cache_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_CACHE_MANAGER_H