#include "src/server/chunk_server/file_chunk_manager.h"

#include "gtest/gtest.h"

using namespace dfs::server;

class FileChunkManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        fileChunkManager_ = FileChunkManager::GetInstance();
        fileChunkManager_->Initialize("./gtest_file_chunks");
    }

    FileChunkManager* fileChunkManager_;
};

TEST_F(FileChunkManagerTest, RunTest) {
    const std::string& chunk_handle = "0";
    fileChunkManager_->CreateChunk(chunk_handle, 1);

    const std::string& data = "abcdefghijk";
    auto write_len_or = fileChunkManager_->WriteToChunk(chunk_handle, 0, data.size(), data);
    EXPECT_TRUE(write_len_or.ok());
    EXPECT_EQ(write_len_or.value(), data.size());
    auto read_data_or = fileChunkManager_->ReadFromChunk(chunk_handle, 0, data.size());
    EXPECT_TRUE(read_data_or.ok());
    EXPECT_EQ(read_data_or.value(), data);
    auto delete_status = fileChunkManager_->DeleteChunk(chunk_handle);
    EXPECT_TRUE(delete_status.ok());
}