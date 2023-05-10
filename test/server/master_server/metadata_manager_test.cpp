#include "src/server/master_server/metadata_manager.h"

#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace dfs::server;

class MetadataManagerTest : public ::testing::Test {
   protected:
    void SetUp() override { metadataManager_ = MetadataManager::GetInstance(); }

    MetadataManager* metadataManager_;
};

TEST_F(MetadataManagerTest, CreateFileMetadata) {
    EXPECT_FALSE(metadataManager_->ExistFileMetadata("/foo"));

    auto create_metadata = metadataManager_->CreateFileMetadata("/foo");
    EXPECT_TRUE(create_metadata.ok());
    EXPECT_TRUE(metadataManager_->ExistFileMetadata("/foo"));

    auto foo_metadata_or = metadataManager_->GetFileMetadata("/foo");
    EXPECT_TRUE(foo_metadata_or.ok());
    auto foo_metadata = foo_metadata_or.value();
    EXPECT_EQ(foo_metadata->filename(), "/foo");

    auto first_chunk_handle_or = metadataManager_->CreateChunkHandle("/foo", 0);
    EXPECT_TRUE(first_chunk_handle_or.ok());
    auto first_chunk_handle = first_chunk_handle_or.value();
    EXPECT_EQ(first_chunk_handle, "0");
    EXPECT_EQ(foo_metadata->chunk_handles_size(), 1);
}

// 多线程同时创建多个文件，确保每个文件都正确创建
// 同时测试 chunk_handle 的唯一性
TEST_F(MetadataManagerTest, CreateFileMetadataInParallel) {
    int numOfThreads = 100;

    std::vector<std::thread> threads;
    for (int i = 0; i < numOfThreads; i++) {
        threads.push_back(std::thread([&, i] {
            metadataManager_->CreateFileMetadata("/" + std::to_string(i));
            metadataManager_->CreateChunkHandle("/" + std::to_string(i), 0);
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    threads.clear();

    std::set<std::string> uset;

    for (int i = 0; i < numOfThreads; i++) {
        const std::string& filename = "/" + std::to_string(i);
        EXPECT_TRUE(metadataManager_->ExistFileMetadata(filename));
        auto file_metadata_or = metadataManager_->GetFileMetadata(filename);
        EXPECT_TRUE(file_metadata_or.ok());
        auto file_metadata = file_metadata_or.value();
        EXPECT_EQ(file_metadata->filename(), filename);

        auto& chunk_handles = *file_metadata->mutable_chunk_handles();
        EXPECT_EQ(chunk_handles.count(0), 1);

        uset.insert(chunk_handles[0]);
    }

    // 确保 uuid 创建的数量与新建文件的数量相同
    EXPECT_EQ(uset.size(), numOfThreads);
}

// 多个线程同时创建同一个文件，确保只有一个线程成功创建
TEST_F(MetadataManagerTest, CreateSameFileMetadataInParallel) {
    const std::string& filename = "/sameFile";
    int numOfThreads = 1000;

    std::atomic<int> count = 0;

    std::vector<std::thread> threads;
    for (int i = 0; i < numOfThreads; i++) {
        threads.push_back(std::thread([&, i] {
            auto status = metadataManager_->CreateFileMetadata(filename);
            if (status.ok()) {
                count++;
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    EXPECT_EQ(count.load(), 1);

    for (int i = 0; i < numOfThreads; i++) {
        threads.push_back(std::thread([&, i] {
            auto status_or = metadataManager_->CreateChunkHandle(filename, i);
            // EXPECT_EQ(status_or.value(), std::to_string(i));
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }
    threads.clear();

    std::set<std::string> uset;
    auto file_metadata_or = metadataManager_->GetFileMetadata(filename);
    EXPECT_TRUE(file_metadata_or.ok());
    auto file_metadata = file_metadata_or.value();
    auto& chunk_handles = *file_metadata->mutable_chunk_handles();
    EXPECT_EQ(chunk_handles.size(), numOfThreads);
    // EXPECT_EQ(metadataManager_->count_.load(), numOfThreads);
    // EXPECT_EQ(metadataManager_->count_.load(), numOfThreads + 1);

    for (int i = 0; i < numOfThreads; i++) {
        uset.insert(chunk_handles[i]);
    }

    EXPECT_EQ(uset.size(), numOfThreads);
}

// TEST_F(MetadataManagerTest, CreateFileInParallelWithOverlap) {
//     int numOfThread = 2;
//     int numChunkPerFile = 100;

//     const std::string filename = "/CreateFileInParallelWithOverlap";

//     // 确保文件存在
//     auto create_file_metadata_or =
//     metadataManager_->CreateFileMetadata(filename);
//     EXPECT_TRUE(create_file_metadata_or.ok());

//     std::atomic<int> count = 0;

//     std::vector<std::thread> threads;
//     for (int i = 0; i < numOfThread; i++) {
//         threads.push_back(std::thread([&, i] () {
//             for (int chunk_id = 0; chunk_id < numChunkPerFile; chunk_id++) {
//                 auto status_or =
//                 metadataManager_->CreateChunkHandle(filename, chunk_id); if
//                 (status_or.ok()) {
//                     count++;
//                 } else {
//                     EXPECT_TRUE(google::protobuf::util::IsAlreadyExists(status_or.status()));
//                 }
//             }
//         }));
//     }

//     for (auto& thread: threads) {
//         thread.join();
//     }

//     threads.clear();

//     EXPECT_EQ(count.load(), numChunkPerFile);

//     auto file_metadata_or = metadataManager_->GetFileMetadata(filename);
//     EXPECT_TRUE(file_metadata_or.ok());
//     auto file_metadata = file_metadata_or.value();
//     EXPECT_EQ(file_metadata->chunk_handles_size(), numChunkPerFile);
// }