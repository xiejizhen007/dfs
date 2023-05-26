#include <grpcpp/grpcpp.h>

#include <memory>

#include "src/common/config_manager.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/server/chunk_server/chunk_server_control_service_impl.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"

using dfs::common::ConfigManager;
using dfs::server::ChunkServerControlServiceImpl;
using dfs::server::ChunkServerFileServiceImpl;
using dfs::server::ChunkServerImpl;
using dfs::server::FileChunkManager;

int main(int argc, char* argv[]) {
    // 初始化日志
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);

    if (argc != 2) {
        LOG(ERROR) << "parameter error, try like this: command chunk_server0";
        return -1;
    }

    const std::string chunk_server_name = argv[1];

    // CMAKE_SOURCE_DIR 是从 cmake 设置的宏
    const std::string config_path =
        std::string(CMAKE_SOURCE_DIR) + "/config.json";

    if (!ConfigManager::GetInstance()->InitConfigManager(config_path)) {
        LOG(ERROR) << "config init error, check config path";
        return 1;
    }

    auto address =
        ConfigManager::GetInstance()->GetChunkServerAddress(chunk_server_name);
    auto port =
        ConfigManager::GetInstance()->GetChunkServerPort(chunk_server_name);

    grpc::ServerBuilder builder;
    std::string server_address = address + ":" + std::to_string(port);
    LOG(INFO) << chunk_server_name << " listen on " << server_address;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.SetMaxReceiveMessageSize(
        ConfigManager::GetInstance()->GetBlockSize() * dfs::common::bytesMB +
        1000);

    // initialize file chunk manager
    FileChunkManager::GetInstance()->Initialize(
        chunk_server_name,
        dfs::common::bytesMB * ConfigManager::GetInstance()->GetBlockSize());

    ChunkServerControlServiceImpl control_service;
    builder.RegisterService(&control_service);

    ChunkServerFileServiceImpl file_service;
    builder.RegisterService(&file_service);

    auto chunk_server_impl = ChunkServerImpl::GetInstance();
    chunk_server_impl->Initialize(chunk_server_name,
                                  ConfigManager::GetInstance());
    chunk_server_impl->RegisterMasterServerClient("127.0.0.1:50050");

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    // set up start task
    chunk_server_impl->StartReportToMaster();

    server->Wait();

    // server is over
    chunk_server_impl->StopReportToMaster();
    google::ShutdownGoogleLogging();
    return 0;
}