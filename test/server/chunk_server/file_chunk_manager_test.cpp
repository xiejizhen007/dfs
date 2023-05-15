#include "src/server/chunk_server/file_chunk_manager.h"

#include "gtest/gtest.h"

using namespace dfs::server;

class FileChunkManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        fileChunkManager_ = FileChunkManager::GetInstance();
        fileChunkManager_->Initialize("./gtest_file_chunks", dfs::common::bytesMB * 4);
    }

    FileChunkManager* fileChunkManager_;
};

TEST_F(FileChunkManagerTest, RunTest) {
    const std::string& chunk_handle = "0";
    const std::string& data = "abcdefghijk";
    uint32_t version = 1;

    // unable to read non-exist chunk
    auto read_non_exist_or =
        fileChunkManager_->ReadFromChunk(chunk_handle, version, 0, data.size());
    EXPECT_FALSE(read_non_exist_or.ok());

    // create chunk to read, write and delete chunk
    auto create_status = fileChunkManager_->CreateChunk(chunk_handle, version);
    EXPECT_TRUE(create_status.ok());

    auto write_len_or = fileChunkManager_->WriteToChunk(chunk_handle, version,
                                                        0, data.size(), data);
    EXPECT_TRUE(write_len_or.ok());
    EXPECT_EQ(write_len_or.value(), data.size());

    // update the version
    auto update_status = fileChunkManager_->UpdateChunkVersion(
        chunk_handle, version, version + 1);
    EXPECT_TRUE(update_status.ok());

    // read the wrong version
    auto read_data_or =
        fileChunkManager_->ReadFromChunk(chunk_handle, version, 0, data.size());
    EXPECT_FALSE(read_data_or.ok());

    // read the right version
    version++;
    read_data_or =
        fileChunkManager_->ReadFromChunk(chunk_handle, version, 0, data.size());
    EXPECT_TRUE(read_data_or.ok());
    // the data read is the same as the data written
    EXPECT_EQ(read_data_or.value(), data);

    // delete the chunk
    auto delete_status = fileChunkManager_->DeleteChunk(chunk_handle);
    EXPECT_TRUE(delete_status.ok());

    // read and write non-exist chunk
    read_data_or =
        fileChunkManager_->ReadFromChunk(chunk_handle, version, 0, data.size());
    EXPECT_FALSE(read_data_or.ok());

    write_len_or = fileChunkManager_->WriteToChunk(chunk_handle, version, 0,
                                                   data.size(), data);
    EXPECT_FALSE(write_len_or.ok());
}