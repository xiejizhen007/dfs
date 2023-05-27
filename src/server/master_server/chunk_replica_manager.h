#ifndef DFS_SERVER_CHUNK_REPLICA_MANAGER_H
#define DFS_SERVER_CHUNK_REPLICA_MANAGER_H

#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>

#include <atomic>
#include <memory>
#include <queue>
#include <string>
#include <thread>

#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

namespace dfs {
namespace server {

class ChunkReplicaManager {
   public:
    // 单例模式
    static ChunkReplicaManager* GetInstance();

    void AddChunkReplicaTask(const std::string& chunk_handle);

    void StartChunkReplicaCopyTask();

    void StopChunkReplicaCopyTask();

   private:
    ChunkReplicaManager();

    // 块句柄队列，将数据块副本数量低于健康值的块句柄记录在这里
    // 生产者消费者队列
    std::queue<std::string> chunk_replica_queues_;

    // 锁
    absl::Mutex chunk_replica_queues_lock_;

    // 条件变量
    absl::CondVar cond_var_;

    // 用于执行块副本复制任务
    std::unique_ptr<std::thread> thread_ptr_;

    // 用于副本探测
    std::unique_ptr<std::thread> replica_check_thread_;

    // 是否暂停当前副本复制任务
    std::atomic<bool> stop_chunk_replica_copy_task_{false};

    // 管理块服务器的对象
    ChunkServerManager* chunk_server_manager_;

    // 管理元数据的单例对象
    MetadataManager* metadata_manager_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_REPLICA_MANAGER_H