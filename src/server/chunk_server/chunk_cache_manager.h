#ifndef DFS_SERVER_CHUNK_CACHE_MANAGER_H
#define DFS_SERVER_CHUNK_CACHE_MANAGER_H

#include <google/protobuf/stubs/statusor.h>

#include "src/common/utils.h"

namespace dfs {
namespace server {

class ChunkCacheManager {
   public:
    // 获取单例对象
    static ChunkCacheManager* GetInstance();

    // 通过校验和获取对应数据
    google::protobuf::util::StatusOr<std::string> Get(const std::string& key);

    // 以校验和为键，数据为值，添加至缓存中
    google::protobuf::util::Status Set(const std::string& key,
                                       const std::string& value);

    // 从缓存中清除掉校验和对应的数据
    google::protobuf::util::Status Remove(const std::string& key);

   private:
    ChunkCacheManager() = default;
    dfs::common::parallel_hash_map<std::string, std::string> cache_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_CACHE_MANAGER_H