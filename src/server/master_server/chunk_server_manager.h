#ifndef DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <memory>

#include "chunk_server.pb.h"

namespace protos {
bool operator==(const ChunkServerLocation& l, const ChunkServerLocation& r);

bool operator==(const ChunkServer& l, const ChunkServer& r);

bool operator<(const ChunkServer& l, const ChunkServer& r);
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

using ChunkServerLocationFlatSet =
    absl::flat_hash_set<protos::ChunkServerLocation, ChunkServerLocationHash>;

class ChunkServerManager {
   public:
   // 获取单例对象
    static ChunkServerManager* GetInstance();

    // 注册块服务器
    bool RegisterChunkServer(std::shared_ptr<protos::ChunkServer> chunk_server);

    // 注销块服务器
    bool UnRegisterChunkServer(const protos::ChunkServerLocation& location);

    // 根据服务器地址获取块服务器
    std::shared_ptr<protos::ChunkServer> GetChunkServer(
        const protos::ChunkServerLocation& location);

    // 获取存储块句柄的块服务器地址集合
    ChunkServerLocationFlatSet GetChunkLocation(
        const std::string& chunk_handle);

    // 分配一定数量的块服务器用于存储块句柄
    ChunkServerLocationFlatSet AssignChunkServer(
        const std::string& chunk_handle, const uint32_t& server_request_nums);

    // 更新块服务器信息
    void UpdateChunkServer(
        const protos::ChunkServerLocation& location,
        const uint32_t& available_disk_mb,
        const absl::flat_hash_set<std::string>& chunks_to_add,
        const absl::flat_hash_set<std::string>& chunks_to_remove);

   private:
    ChunkServerManager() = default;

    // map location to server
    absl::flat_hash_map<protos::ChunkServerLocation,
                        std::shared_ptr<protos::ChunkServer>,
                        ChunkServerLocationHash>
        chunk_server_maps_;

    absl::Mutex chunk_server_maps_lock_;

    // store chunk location in memory
    absl::flat_hash_map<std::string, ChunkServerLocationFlatSet>
        chunk_location_maps_;

    absl::Mutex chunk_location_maps_lock_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H