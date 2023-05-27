#include "src/common/system_logger.h"
#include "src/server/master_server/chunk_server_heartbeat_task.h"
#include "src/server/master_server/chunk_server_manager_service_impl.h"
#include "src/server/master_server/master_metadata_service_impl.h"
#include "src/server/master_server/chunk_replica_manager.h"

using namespace dfs::server;

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
    LOG(INFO) << "start master server";

    grpc::ServerBuilder builder;
    std::string server_address("0.0.0.0:50050");
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // set up service
    ChunkServerManagerServiceImpl chunk_server_manager_service;
    builder.RegisterService(&chunk_server_manager_service);

    MasterMetadataServiceImpl metadata_service;
    builder.RegisterService(&metadata_service);

    // build server
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    LOG(INFO) << "master server listening on " << server_address;

    // start heart beat task
    ChunkServerHeartBeatTask::GetInstance()->StartHeartBeatTask();

    // start copy task
    ChunkReplicaManager::GetInstance()->StartChunkReplicaCopyTask();

    server->Wait();

    google::ShutdownGoogleLogging();
    return 0;
}