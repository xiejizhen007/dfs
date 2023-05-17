#include "src/server/master_server/chunk_server_manager_service_impl.h"

#include <gtest/gtest.h>

#include "src/grpc_client/chunk_server_manager_service_client.h"

using namespace dfs::grpc_client;
using protos::grpc::ReportChunkServerRequest;

class ChunkServerManagerServiceImplTest : public ::testing::Test {};

TEST_F(ChunkServerManagerServiceImplTest, TalkToMaster) {
    const std::string server_address = "127.0.0.1:50050";

    std::shared_ptr<ChunkServerManagerServiceClient> client =
        std::make_shared<ChunkServerManagerServiceClient>(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials()));

    ReportChunkServerRequest request;
    request.mutable_chunk_server()->mutable_location()->set_server_hostname("127.0.0.1");
    request.mutable_chunk_server()->mutable_location()->set_server_port(50100);
    auto respond_or = client->SendRequest(request);
    std::cout << respond_or.status().ToString() << std::endl;
    EXPECT_TRUE(respond_or.ok());
}