#ifndef DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H
#define DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <memory>
#include <vector>

#include "chunk_server.pb.h"
#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/grpc_client/chunk_server_lease_service_client.h"

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

// 辅助函数，将 set 转化为 vector
std::vector<protos::ChunkServerLocation> ChunkServerLocationFlatSetToVector(
    const ChunkServerLocationFlatSet& location_set);

// 将块服务器位置转化为 "ip:port" 的字符串格式
std::string ChunkServerLocationToString(const protos::ChunkServerLocation& location);

/**
 * 块服务器管理
 * 1. 管理着所以块服务器信息
 * 2. 管理与块服务器通信的客户端
*/
class ChunkServerManager {
    // 声明友元类，以便访问私有变量
    friend class ChunkServerHeartBeatTask;

    friend class ChunkReplicaManager;

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

    size_t GetChunkLocationSize(const std::string& chunk_handle);

    // 获取存储块句柄的块服务器地址集合
    ChunkServerLocationFlatSet GetChunkLocationNoLock(
        const std::string& chunk_handle);

    // 分配一定数量的块服务器用于存储块句柄
    ChunkServerLocationFlatSet AssignChunkServer(
        const std::string& chunk_handle, const uint32_t& server_request_nums);

    // 分配一定数量的块服务器，用于执行数据块副本复制
    ChunkServerLocationFlatSet AssignChunkServerToCopyReplica(
        const std::string& chunk_handle, const uint32_t& healthy_replica_nums);

    // 更新块服务器信息
    void UpdateChunkServer(
        const protos::ChunkServerLocation& location,
        const uint32_t& available_disk_mb,
        const absl::flat_hash_set<std::string>& chunks_to_add,
        const absl::flat_hash_set<std::string>& chunks_to_remove);

    std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
    GetOrCreateChunkServerFileServiceClient(const std::string& server_address);

    std::shared_ptr<dfs::grpc_client::ChunkServerLeaseServiceClient>
    GetOrCreateChunkServerLeaseServiceClient(const std::string& server_address);

   private:
    ChunkServerManager() = default;

    // 更新数据块元数据的主副本服务器位置信息
    void UpdateFileChunkMetadataLocation(
        const std::string& chunk_handle,
        const protos::ChunkServerLocation& location);

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

    // 块服务器文件服务的客户端映射表
    absl::flat_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>>
        chunk_server_file_service_clients_;

    // 客户端映射表的互斥锁
    absl::Mutex chunk_server_file_service_clients_lock_;

    // 与租约服务进行通信的客户端
    absl::flat_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerLeaseServiceClient>>
        chunk_server_lease_service_clients_;

    // 客户端映射表的互斥锁
    absl::Mutex chunk_server_lease_service_clients_lock_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_SERVER_CHUNK_SERVER_MANAGER_H