#include "src/server/master_server/chunk_replica_manager.h"

#include "src/common/system_logger.h"

namespace dfs {
namespace server {

using protos::grpc::ChunkReplicaCopyRequest;
using protos::grpc::ChunkReplicaCopyRespond;

ChunkReplicaManager::ChunkReplicaManager() {
    chunk_server_manager_ = ChunkServerManager::GetInstance();
    metadata_manager_ = MetadataManager::GetInstance();
}

ChunkReplicaManager* ChunkReplicaManager::GetInstance() {
    static ChunkReplicaManager* instance = new ChunkReplicaManager();
    return instance;
}

void ChunkReplicaManager::AddChunkReplicaTask(const std::string& chunk_handle) {
    absl::WriterMutexLock chunk_replica_queues_lock_guard(
        &chunk_replica_queues_lock_);
    chunk_replica_queues_.push(chunk_handle);
    cond_var_.Signal();
}

void ChunkReplicaManager::StartChunkReplicaCopyTask() {
    thread_ptr_ = std::make_unique<std::thread>(std::thread([&]() {
        while (!stop_chunk_replica_copy_task_.load()) {
            // TODO: 执行副本复制任务

            chunk_replica_queues_lock_.Lock();
            while (chunk_replica_queues_.empty()) {
                cond_var_.Wait(&chunk_replica_queues_lock_);
            }

            // 条件变量中重新赋予了锁
            const std::string chunk_handle = chunk_replica_queues_.front();
            chunk_replica_queues_.pop();
            // 解锁，以便继续添加任务
            chunk_replica_queues_lock_.Unlock();

            LOG(INFO) << "start to copy chunk handle " << chunk_handle;

            // TODO: use config
            if (chunk_server_manager_->GetChunkLocationSize(chunk_handle) >=
                3) {
                continue;
            }

            // 副本数量小于健康值
            auto locations =
                chunk_server_manager_->AssignChunkServerToCopyReplica(
                    chunk_handle, 3);
            if (locations.empty()) {
                LOG(INFO) << "no more chunk server to copy " << chunk_handle
                          << " replica";
                continue;
            }

            // 向数据块主副本服务器发出数据块复制请求，让主副本服务器将数据块复制到块服务器上
            auto metadata_or =
                metadata_manager_->GetFileChunkMetadata(chunk_handle);
            if (!metadata_or.ok()) {
                LOG(ERROR) << "ChunkReplicaCopyTask, can not get chunk "
                              "metadata, because "
                           << metadata_or.status().ToString();
                continue;
            }

            const std::string primary_server_address =
                metadata_or.value().primary_location().server_hostname() + ":" +
                std::to_string(
                    metadata_or.value().primary_location().server_port());
            auto primary_client =
                chunk_server_manager_->GetOrCreateChunkServerFileServiceClient(
                    primary_server_address);
            if (!primary_client) {
                LOG(ERROR) << "can not get or create primary server file "
                              "service client";
                continue;
            }

            ChunkReplicaCopyRequest copy_req;
            copy_req.set_chunk_handle(chunk_handle);
            for (auto location : locations) {
                *copy_req.add_locations() = location;
            }

            auto respond = primary_client->SendRequest(copy_req);
            if (respond.ok()) {
                LOG(INFO) << "copy right";
            }
        }
    }));

    // 副本探测
    replica_check_thread_ = std::make_unique<std::thread>(std::thread([&]() {
        while (!stop_chunk_replica_copy_task_.load()) {
            // 探测副本数量，然后执行复制任务
            for (const auto& val_pair :
                 chunk_server_manager_->chunk_location_maps_) {
                if (val_pair.second.size() < 3) {
                    LOG(INFO) << "ReplicaCheckTask: chunk handle "
                              << val_pair.first << " need to copy";
                    AddChunkReplicaTask(val_pair.first);
                }
            }

            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }));
}

void ChunkReplicaManager::StopChunkReplicaCopyTask() {
    stop_chunk_replica_copy_task_.store(true);
    thread_ptr_->join();
    replica_check_thread_->join();
}

}  // namespace server
}  // namespace dfs
