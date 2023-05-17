#include <memory>

#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
#include "src/common/utils.h"
#include "src/server/chunk_server/chunk_server_control_service_impl.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/chunk_server_impl.h"

using dfs::server::ChunkServerControlServiceImpl;
using dfs::server::ChunkServerFileServiceImpl;
using dfs::server::ChunkServerImpl;
using dfs::server::FileChunkManager;

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);

    grpc::ServerBuilder builder;
    std::string server_address("0.0.0.0:50100");
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // initialize file chunk manager
    FileChunkManager::GetInstance()->Initialize("chunk_server_0",
                                                dfs::common::bytesMB * 4);

    ChunkServerControlServiceImpl control_service;
    builder.RegisterService(&control_service);

    ChunkServerFileServiceImpl file_service;
    builder.RegisterService(&file_service);

    auto chunk_server_impl = ChunkServerImpl::GetInstance();
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