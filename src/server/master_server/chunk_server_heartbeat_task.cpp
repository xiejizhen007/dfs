#include "src/server/master_server/chunk_server_heartbeat_task.h"

namespace dfs {
namespace server {

ChunkServerHeartBeatTask::ChunkServerHeartBeatTask() {}

ChunkServerHeartBeatTask::~ChunkServerHeartBeatTask() {}

ChunkServerHeartBeatTask* ChunkServerHeartBeatTask::GetInstance() {
    static ChunkServerHeartBeatTask* instance = new ChunkServerHeartBeatTask();
    return instance;
}

void ChunkServerHeartBeatTask::StartHeartBeatTask() {
    thread_ = std::make_unique<std::thread>(std::thread([&]() {
        while (!stop_heart_beat_task_.load()) {
            // get server address

            // get control client

            // check respond
        }
    }));
}

void ChunkServerHeartBeatTask::StopHeartBeatTask() {}

std::shared_ptr<dfs::grpc_client::ChunkServerControlServiceClient>
ChunkServerHeartBeatTask::GetOrCreateChunkServerControlServiceClient(
    const std::string& server_address) {
    return control_clients_[server_address];
}

}  // namespace server
}  // namespace dfs