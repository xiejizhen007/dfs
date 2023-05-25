#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include <gtest/gtest.h>

#include <thread>

#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"

const std::string test_chunk_handle = "0123456789";
const uint32_t test_chunk_version = 2;
const std::string test_data = "chunk_server_file_service_impl_test";
const std::string test_server_address = "0.0.0.0:55005";

using namespace dfs::server;
using namespace dfs::grpc_client;

using dfs::server::ChunkServerFileServiceImpl;
using grpc::Server;
using grpc::ServerBuilder;
using protos::grpc::InitFileChunkRequest;
using protos::grpc::InitFileChunkRespond;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::ReadFileChunkRespond;
using protos::grpc::WriteFileChunkRequest;
using protos::grpc::WriteFileChunkRequestHeader;
using protos::grpc::WriteFileChunkRespond;

void InitChunk() {
    FileChunkManager* file_ = FileChunkManager::GetInstance();
    file_->CreateChunk(test_chunk_handle, test_chunk_version);
    file_->WriteToChunk(test_chunk_handle, test_chunk_version, 0,
                        test_data.size(), test_data);
}

void BuildServer() {
    // 确保文件系统正确性
    FileChunkManager::GetInstance()->Initialize(
        "chunk_server_file_service_impl_test", 1024);

    ChunkServerFileServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(test_server_address,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << test_server_address << std::endl;
    server->Wait();
}

ReadFileChunkRequest MakeVaildReadFileChunkRequest() {
    ReadFileChunkRequest request;
    request.set_chunk_handle(test_chunk_handle);
    request.set_version(test_chunk_version);
    request.set_offset(0);
    request.set_length(test_data.length());
    return request;
}

class ChunkServerFileServiceImplTest : public ::testing::Test {
   protected:
    void SetUp() override {
        chunk_server_file_service_client_ =
            std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
                test_server_address, grpc::InsecureChannelCredentials()));
    }

    std::shared_ptr<ChunkServerFileServiceClient>
        chunk_server_file_service_client_;
};

// 创建新的数据块
TEST_F(ChunkServerFileServiceImplTest, InitNewFileChunkTest) {
    InitFileChunkRequest request;
    request.set_chunk_handle("init_new_file_chunk_test");
    auto respond_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
    EXPECT_EQ(respond_or.value().status(),
              protos::grpc::InitFileChunkRespond::CREATED);

    // 清理数据块，保证下一次测试的正确性
    FileChunkManager::GetInstance()->DeleteChunk("init_new_file_chunk_test");
}

// 创建一个已存在的数据块
TEST_F(ChunkServerFileServiceImplTest, InitFileChunkTest) {
    InitFileChunkRequest request;
    request.set_chunk_handle(test_chunk_handle);
    auto respond_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
    EXPECT_EQ(respond_or.value().status(),
              protos::grpc::InitFileChunkRespond::ALREADY_EXISTS);
}

// 一次正确的读操作
TEST_F(ChunkServerFileServiceImplTest, ReadChunkTest) {
    auto request = MakeVaildReadFileChunkRequest();
    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), test_data.size());
    EXPECT_EQ(status_or.value().data(), test_data);
}

// 读取部分数据
TEST_F(ChunkServerFileServiceImplTest, ReadChunkPartTest) {
    auto request = MakeVaildReadFileChunkRequest();
    uint32_t read_length = 5;
    request.set_length(read_length);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), read_length);
    EXPECT_EQ(status_or.value().data(), test_data.substr(0, read_length));
}

// 读取过量数据，超出数据块范围
TEST_F(ChunkServerFileServiceImplTest, ReadChunkBigLengthTest) {
    auto request = MakeVaildReadFileChunkRequest();
    uint32_t read_length = test_data.size() + 5;
    request.set_length(read_length);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), test_data.size());
    EXPECT_EQ(status_or.value().data(), test_data);
}

// 读取经过偏移的数据
TEST_F(ChunkServerFileServiceImplTest, ReadChunkOffsetTest) {
    auto request = MakeVaildReadFileChunkRequest();
    uint32_t offset = 5;
    uint32_t read_length = test_data.size() - offset;
    request.set_offset(offset);
    request.set_length(read_length);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), read_length);
    EXPECT_EQ(status_or.value().data(), test_data.substr(offset, read_length));
}

// 读取经过偏移的数据
TEST_F(ChunkServerFileServiceImplTest, ReadChunkOffsetPartTest) {
    auto request = MakeVaildReadFileChunkRequest();
    uint32_t offset = 5;
    uint32_t read_length = 5;
    request.set_offset(offset);
    request.set_length(read_length);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), read_length);
    EXPECT_EQ(status_or.value().data(), test_data.substr(offset, read_length));
}

// 读取经过偏移的过量数据
TEST_F(ChunkServerFileServiceImplTest, ReadChunkOffsetBigLengthTest) {
    auto request = MakeVaildReadFileChunkRequest();
    uint32_t offset = 5;
    request.set_offset(offset);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OK);
    EXPECT_EQ(status_or.value().read_length(), test_data.size() - offset);
    EXPECT_EQ(status_or.value().data(), test_data.substr(offset));
}

// 读取范围之外的数据
TEST_F(ChunkServerFileServiceImplTest, ReadChunkOutOfRangeTest) {
    auto request = MakeVaildReadFileChunkRequest();
    request.set_offset(test_data.size() + 1);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::OUT_OF_RANGE);
}

// 读取不存在的数据块
TEST_F(ChunkServerFileServiceImplTest, ReadChunkNotFoundTest) {
    auto request = MakeVaildReadFileChunkRequest();
    request.set_chunk_handle("chunk_not_exist");

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::NOT_FOUND);
}

// 读取错误版本的数据块
TEST_F(ChunkServerFileServiceImplTest, ReadChunkBadVersionTest) {
    auto request = MakeVaildReadFileChunkRequest();
    request.set_version(test_chunk_version + 10);

    auto status_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(status_or.ok());
    EXPECT_EQ(status_or.value().status(), ReadFileChunkRespond::VERSION_ERROR);
}



int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::thread server_thread = std::thread(BuildServer);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    InitChunk();

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    // Clean up background server
    pthread_cancel(server_thread.native_handle());
    server_thread.join();

    return exit_code;
}