#include "src/server/master_server/chunk_server_manager.h"
#include "gtest/gtest.h"
#include <vector>
#include <thread>
#include <unordered_map>

using namespace dfs::server;
using namespace protos;

class ChunkServerManagerTest : public ::testing::Test {
   protected:
    void SetUp() override { chunk_server_manager_ = ChunkServerManager::GetInstance(); }

    ChunkServerManager* chunk_server_manager_;
};

ChunkServerLocation CreateChunkServerLocation(const std::string& hostname, const uint32_t& port) {
    ChunkServerLocation location;
    location.set_server_hostname(hostname);
    location.set_server_port(port);
    return location;
}

TEST_F(ChunkServerManagerTest, BasicFunctionTest) {
    auto x = chunk_server_manager_->GetChunkLocation("non");

    std::shared_ptr<ChunkServer> server(new ChunkServer());
    ChunkServerLocation location = CreateChunkServerLocation("127.0.0.1", 1234);
    EXPECT_FALSE(server->has_location());
    *server->mutable_location() = location;
    EXPECT_TRUE(server->has_location());
    server->mutable_stored_chunk_handles()->Add("0");
    server->mutable_stored_chunk_handles()->Add("1");
    EXPECT_EQ(server->stored_chunk_handles_size(), 2);
    EXPECT_EQ(server->location(), location);

    chunk_server_manager_->RegisterChunkServer(server);

    auto xx = chunk_server_manager_->GetChunkLocation("0");
    EXPECT_TRUE(xx.Contains(location));

    auto new_server = chunk_server_manager_->GetChunkServer(location);
    if (!new_server) {
        std::cout << "get chunk server failed" << std::endl;
    } else {
        std::cout << server->location().server_hostname() << std::endl;
        std::cout << new_server->location().server_hostname() << std::endl;
    }

    chunk_server_manager_->UnRegisterChunkServer(location);
    auto xxx = chunk_server_manager_->GetChunkLocation("0");
    EXPECT_FALSE(xxx.Contains(location));

    // std::unordered_map<std::string, int> map;
    // std::atomic<int> insert_count = 0;

    // std::vector<std::thread> threads;

    // int num = 100;

    // for (int i = 0; i < num; i++) {
    //     threads.push_back(std::thread([&, i](){
    //         map.insert({std::to_string(i), i});
    //         insert_count.fetch_add(i);
    //     }));
    // }

    // for (auto& thread: threads) {
    //     thread.join();
    // }

    // threads.clear();

    // int count = 0;
    // for (const auto& obj: map) {
    //     count += obj.second;
    // }

    // EXPECT_EQ(count, insert_count.load());
}