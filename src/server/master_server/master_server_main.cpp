#include "src/common/system_logger.h"
#include "src/grpc_client/chunk_server_control_service_client.h"

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
    LOG(INFO) << "hello";

    std::string target_str = "localhost:50051";
    dfs::grpc_client::ChunkServerControlServiceClient stub(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

    protos::grpc::SendHeartBeatRequest request;
    auto status = stub.SendHeartBeat(request);
    if (status.ok()) {
        LOG(INFO) << "chunk server is still alive";
    }

    google::ShutdownGoogleLogging();
    return 0;
}