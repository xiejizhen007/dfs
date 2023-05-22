#include "src/server/chunk_server/chunk_server_lease_service_impl.h"

#include <gtest/gtest.h>

#include "src/common/config_manager.h"
#include "src/grpc_client/chunk_server_lease_service_client.h"
#include "src/server/chunk_server/chunk_server_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

using dfs::common::ConfigManager;
using dfs::grpc_client::ChunkServerLeaseServiceClient;
using dfs::server::ChunkServerImpl;
using dfs::server::ChunkServerLeaseServiceImpl;
using dfs::server::FileChunkManager;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::GrantLeaseRequest;
using protos::grpc::GrantLeaseRespond;
using protos::grpc::RevokeLeaseRequest;
using protos::grpc::RevokeLeaseRespond;

const std::string TestServerName = "chunk_server0";
const std::string TestServerAddress = "127.0.0.1:50200";

const std::string GrantLeaseChunkHandle = "grantleasechunkhandle";
const std::string RevokeLeaseChunkHandle = "revokeleasechunkhandle";
const uint32_t TestVersion = 2;

const std::string ConfigPath = std::string(CMAKE_SOURCE_DIR) + "/config.json";

const uint64_t TestExpirationUnixSeconds =
    absl::ToUnixSeconds(absl::Now() + absl::Hours(1));

void BuildChunkStore() {
    FileChunkManager::GetInstance()->CreateChunk(GrantLeaseChunkHandle,
                                                 TestVersion);
    FileChunkManager::GetInstance()->CreateChunk(RevokeLeaseChunkHandle,
                                                 TestVersion);

    // 初始化租约
    ChunkServerImpl::GetInstance()->AddOrUpdateLease(GrantLeaseChunkHandle,
                                                     TestExpirationUnixSeconds);
    ChunkServerImpl::GetInstance()->AddOrUpdateLease(RevokeLeaseChunkHandle,
                                                     TestExpirationUnixSeconds);
}

void StartTestServer() {
    FileChunkManager::GetInstance()->Initialize(
        "chunk_server_lease_service_test", 1024);
    ServerBuilder builder;
    builder.AddListeningPort(TestServerAddress,
                             grpc::InsecureServerCredentials());
    ConfigManager::GetInstance()->InitConfigManager(ConfigPath);
    ChunkServerImpl::GetInstance()->Initialize(TestServerName,
                                               ConfigManager::GetInstance());

    BuildChunkStore();

    ChunkServerLeaseServiceImpl lease_service;
    builder.RegisterService(&lease_service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    server->Wait();
}

class ChunkServerLeaseServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        client_ =
            std::make_shared<ChunkServerLeaseServiceClient>(grpc::CreateChannel(
                TestServerAddress, grpc::InsecureChannelCredentials()));
    }

    GrantLeaseRequest MakeGrantLeaseRequest() {
        GrantLeaseRequest request;
        request.set_chunk_handle(GrantLeaseChunkHandle);
        request.set_chunk_version(TestVersion);
        request.mutable_lease_expiration_time()->set_seconds(
            TestExpirationUnixSeconds);
        return request;
    }

    std::shared_ptr<ChunkServerLeaseServiceClient> client_;
};

TEST_F(ChunkServerLeaseServiceTest, GrantLeaseValidRequest) {
    auto request = MakeGrantLeaseRequest();
    auto respond_or = client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
    EXPECT_EQ(respond_or.value().status(), GrantLeaseRespond::ACCEPTED);
}

TEST_F(ChunkServerLeaseServiceTest, GrantLeaseNoChunkHandle) {
    auto request = MakeGrantLeaseRequest();
    request.set_chunk_handle("non_exist_chunk_handle");
    auto respond_or = client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
    EXPECT_EQ(respond_or.value().status(),
              GrantLeaseRespond::REJECTED_NOT_FOUND);
}

TEST_F(ChunkServerLeaseServiceTest, GrantLeaseBadVersion) {
    auto request0 = MakeGrantLeaseRequest();
    request0.set_chunk_version(TestVersion + 1);
    auto respond_or0 = client_->SendRequest(request0);
    EXPECT_TRUE(respond_or0.ok());
    EXPECT_EQ(respond_or0.value().status(),
              GrantLeaseRespond::REJECTED_VERSION);

    auto request1 = MakeGrantLeaseRequest();
    request1.set_chunk_version(TestVersion - 1);
    auto respond_or1 = client_->SendRequest(request1);
    EXPECT_TRUE(respond_or1.ok());
    EXPECT_EQ(respond_or1.value().status(),
              GrantLeaseRespond::REJECTED_VERSION);
}

TEST_F(ChunkServerLeaseServiceTest, GrantLeaseAlreadyExist) {
    auto request = MakeGrantLeaseRequest();
    request.mutable_lease_expiration_time()->set_seconds(
        absl::ToUnixSeconds(absl::Now() - absl::Minutes(5)));
    auto respond_or = client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
    EXPECT_EQ(respond_or.value().status(), GrantLeaseRespond::REJECTED_EXPIRED);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);

    // 后台线程，跑租约服务
    std::thread server_thread = std::thread(StartTestServer);
    // 等到服务启动完成
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    pthread_cancel(server_thread.native_handle());
    server_thread.join();

    return exit_code;
}