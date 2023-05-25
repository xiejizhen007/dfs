#ifndef DFS_SERVER_CHUNK_SERVER_HEARTBEAT_TASK_H
#define DFS_SERVER_CHUNK_SERVER_HEARTBEAT_TASK_H

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <thread>

#include "chunk_server.pb.h"
#include "src/grpc_client/chunk_server_control_service_client.h"

namespace dfs {
namespace server {

class ChunkServerHeartBeatTask {
   public:
    // 获取单例对象
    static ChunkServerHeartBeatTask* GetInstance();

    // 启动心跳包任务
    void StartHeartBeatTask();

    // 结束心跳包任务
    void StopHeartBeatTask();

   private:
    ChunkServerHeartBeatTask();
    ~ChunkServerHeartBeatTask();

    // 执行心跳包检测服务的线程
    std::unique_ptr<std::thread> thread_;

    std::atomic<bool> stop_heart_beat_task_{false};

    std::shared_ptr<dfs::grpc_client::ChunkServerControlServiceClient>
    GetOrCreateChunkServerControlServiceClient(
        const std::string& server_address);

    // map<server_address, client ptr>
    absl::flat_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerControlServiceClient>>
        control_clients_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_HEARTBEAT_TASK_H