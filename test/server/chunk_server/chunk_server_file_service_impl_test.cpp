#include "src/server/chunk_server/chunk_server_file_service_impl.h"

#include <gtest/gtest.h>

#include <thread>

#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"

const std::string test_chunk_handle = "123456789";
const uint32_t test_chunk_version = 1;
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
using protos::grpc::WriteFileChunkRespond;
using protos::grpc::WriteFileChunkRequestHeader;

void BuildServer() {
    ChunkServerFileServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(test_server_address,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << test_server_address << std::endl;
    server->Wait();
}

class ChunkServerFileServiceImplTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FileChunkManager::GetInstance()->Initialize("chunk_server_file_service_impl_test", 1024);

        chunk_server_file_service_client_ =
            std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
                test_server_address, grpc::InsecureChannelCredentials()));
    }

    std::shared_ptr<ChunkServerFileServiceClient>
        chunk_server_file_service_client_;
};

TEST_F(ChunkServerFileServiceImplTest, InitFileChunkTest) {
    InitFileChunkRequest request;
    request.set_chunk_handle(test_chunk_handle);
    auto respond_or = chunk_server_file_service_client_->SendRequest(request);
    EXPECT_TRUE(respond_or.ok());
}

TEST_F(ChunkServerFileServiceImplTest, ReadWriteTest) {
    ReadFileChunkRequest read_request;
    read_request.set_chunk_handle(test_chunk_handle);
    read_request.set_version(test_chunk_version);
    read_request.set_offset(0);
    read_request.set_length(test_data.size());
    auto read_respond_or = chunk_server_file_service_client_->SendRequest(read_request);
    EXPECT_TRUE(read_respond_or.ok());
    auto read_data = read_respond_or.value().data();
    EXPECT_TRUE(read_data == "" || read_data == test_data);
    std::cout << "read data: " << read_data << std::endl;

    WriteFileChunkRequest write_request;
    WriteFileChunkRequestHeader write_header;
    write_header.set_chunk_handle(test_chunk_handle);
    write_header.set_version(test_chunk_version);
    write_header.set_offset(0);
    write_header.set_length(test_data.size());
    write_header.set_data(test_data);
    *write_request.mutable_header() = write_header;
    auto write_respond_or = chunk_server_file_service_client_->SendRequest(write_request);
    EXPECT_TRUE(write_respond_or.ok());
    EXPECT_TRUE(write_respond_or.value().status() == protos::grpc::FileChunkMutationStatus::OK);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::thread server_thread = std::thread(BuildServer);
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    // Clean up background server
    pthread_cancel(server_thread.native_handle());
    server_thread.join();

    return exit_code;
}