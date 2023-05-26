#include "src/server/master_server/chunk_server_heartbeat_task.h"

#include <vector>

#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_manager.h"

namespace dfs {
namespace server {

using dfs::grpc_client::ChunkServerControlServiceClient;
using protos::ChunkServerLocation;
using protos::grpc::SendHeartBeatRequest;
using protos::grpc::SendHeartBeatRespond;

ChunkServerHeartBeatTask::ChunkServerHeartBeatTask() {}

ChunkServerHeartBeatTask::~ChunkServerHeartBeatTask() {}

ChunkServerHeartBeatTask* ChunkServerHeartBeatTask::GetInstance() {
    static ChunkServerHeartBeatTask* instance = new ChunkServerHeartBeatTask();
    return instance;
}

void ChunkServerHeartBeatTask::StartHeartBeatTask() {
    thread_ = std::make_unique<std::thread>(std::thread([&]() {
        // 用于记录被删除的块服务器地址
        std::vector<ChunkServerLocation> location_to_unregister;
        while (!stop_heart_beat_task_.load()) {
            LOG(INFO) << "HeartBeatTask is start";

            // get server address
            for (const auto& location_pair :
                 ChunkServerManager::GetInstance()->chunk_server_maps_) {
                // 块服务器地址
                const std::string server_address =
                    location_pair.first.server_hostname() + ":" +
                    std::to_string(location_pair.first.server_port());

                auto client =
                    GetOrCreateChunkServerControlServiceClient(server_address);
                if (!client) {
                    LOG(ERROR)
                        << "HeartBeatTask: can not get service client to "
                        << server_address;
                    continue;
                }

                LOG(INFO) << "HeartBeatTask: try to talk to server "
                          << server_address;

                SendHeartBeatRequest request;
                google::protobuf::util::Status respond;

                // 与块服务器通信，失败超过 3 次，说明块服务器已经掉线了
                // TODO: use config
                for (int retry = 0; retry < 3; retry++) {
                    respond = client->SendHeartBeat(request);

                    if (respond.ok()) {
                        LOG(INFO) << "chunk server " << server_address
                                  << " is still alive";
                        break;
                    } else {
                        //
                        LOG(ERROR) << "can not talk to server "
                                   << server_address << " " << (retry + 1);
                    }
                }

                if (!respond.ok()) {
                    location_to_unregister.emplace_back(location_pair.first);
                }
            }

            for (const auto& location : location_to_unregister) {
                const std::string server_address =
                    location.server_hostname() + ":" +
                    std::to_string(location.server_port());
                LOG(INFO) << "unregister chunk server: " << server_address;

                // 对于已经掉线的块服务器，将其从系统中注销掉
                ChunkServerManager::GetInstance()->UnRegisterChunkServer(
                    location);

                // 并将与其通信的心跳包客户端删除掉
                control_clients_.erase(server_address);
            }

            location_to_unregister.clear();

            // 线程睡眠
            // TODO: use config
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }));
}

void ChunkServerHeartBeatTask::StopHeartBeatTask() {
    stop_heart_beat_task_.store(false);
    thread_->join();
}

std::shared_ptr<dfs::grpc_client::ChunkServerControlServiceClient>
ChunkServerHeartBeatTask::GetOrCreateChunkServerControlServiceClient(
    const std::string& server_address) {
    if (!control_clients_.contains(server_address)) {
        // create a client to talk to server_address
        LOG(INFO) << "create a heartbeat client to talk to server: "
                  << server_address;
        control_clients_[server_address] =
            std::make_shared<ChunkServerControlServiceClient>(
                grpc::CreateChannel(server_address,
                                    grpc::InsecureChannelCredentials()));
    }

    return control_clients_[server_address];
}

}  // namespace server
}  // namespace dfs