#include <memory>

#include "grpcpp/grpcpp.h"
#include "src/common/system_logger.h"
#include "src/server/chunk_server/chunk_server_control_service_impl.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"

using dfs::server::ChunkServerControlServiceImpl;
using dfs::server::ChunkServerFileServiceImpl;

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);

    grpc::ServerBuilder builder;
    std::string server_address("0.0.0.0:50051");
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    ChunkServerControlServiceImpl control_service;
    builder.RegisterService(&control_service);

    ChunkServerFileServiceImpl file_service;
    builder.RegisterService(&file_service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    server->Wait();

    return 0;
}