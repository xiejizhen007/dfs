#include "src/server/master_server/metadata_manager.h"

#include <google/protobuf/stubs/statusor.h>
#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>
#include <vector>

using namespace dfs::server;

using google::protobuf::util::IsAlreadyExists;
using google::protobuf::util::IsNotFound;

class MetadataManagerTest : public ::testing::Test {
   protected:
    void SetUp() override { metadataManager_ = MetadataManager::GetInstance(); }

    MetadataManager* metadataManager_;
};

// 创建一个文件元数据，并添加数据块
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
            EXPECT_TRUE(status_or.ok());
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
    EXPECT_EQ(file_metadata->chunk_handles_size(), numOfThreads);

    for (int i = 0; i < numOfThreads; i++) {
        uset.insert(chunk_handles[i]);
    }

    EXPECT_EQ(uset.size(), numOfThreads);

    for (int i = 0; i < numOfThreads; i++) {
        EXPECT_TRUE(chunk_handles.contains(i));
    }
    EXPECT_EQ(chunk_handles.size(), numOfThreads);
}

// 多个线程交叉的创建数据块元数据
TEST_F(MetadataManagerTest, CreateFileInParallelWithOverlap) {
    int numOfThread = 50;
    int numChunkPerFile = 100;

    const std::string filename = "/CreateFileInParallelWithOverlap";

    // 确保文件存在
    auto create_file_metadata_or =
        metadataManager_->CreateFileMetadata(filename);
    EXPECT_TRUE(create_file_metadata_or.ok());

    std::atomic<int> count = 0;

    std::vector<std::thread> threads;
    for (int i = 0; i < numOfThread; i++) {
        threads.push_back(std::thread([&, i]() {
            for (int chunk_id = 0; chunk_id < numChunkPerFile; chunk_id++) {
                // 对于文件中每一个数据块，每个线程都会去尝试创建，但只有一个成功
                auto status_or =
                    metadataManager_->CreateChunkHandle(filename, chunk_id);
                if (status_or.ok()) {
                    count++;
                } else {
                    EXPECT_TRUE(google::protobuf::util::IsAlreadyExists(
                        status_or.status()));
                }
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    threads.clear();

    EXPECT_EQ(count.load(), numChunkPerFile);

    auto file_metadata_or = metadataManager_->GetFileMetadata(filename);
    EXPECT_TRUE(file_metadata_or.ok());
    auto file_metadata = file_metadata_or.value();
    EXPECT_EQ(file_metadata->chunk_handles_size(), numChunkPerFile);
}

// 测试一些错误情况
TEST_F(MetadataManagerTest, ErrorTest) {
    // 创建已经存在的文件
    const std::string new_file = "/new_file";
    auto create_metadata = metadataManager_->CreateFileMetadata(new_file);
    EXPECT_TRUE(create_metadata.ok());
    auto create_metadata_again = metadataManager_->CreateFileMetadata(new_file);
    EXPECT_TRUE(!create_metadata_again.ok());
    EXPECT_TRUE(IsAlreadyExists(create_metadata_again));

    // 获取不存在的文件元数据
    const std::string not_exist_file = "/not_exist_file";
    auto not_exist_file_metadata_or =
        metadataManager_->GetFileMetadata(not_exist_file);
    EXPECT_TRUE(!not_exist_file_metadata_or.ok());
    EXPECT_TRUE(IsNotFound(not_exist_file_metadata_or.status()));

    // 获取不存在的文件数据块元数据
    const std::string not_exist_file_chunk = "not_exist_file_chunk";
    auto not_exist_file_chunk_metadata_or =
        metadataManager_->GetFileChunkMetadata(not_exist_file_chunk);
    EXPECT_TRUE(!not_exist_file_chunk_metadata_or.ok());
    EXPECT_TRUE(IsNotFound(not_exist_file_chunk_metadata_or.status()));

    // 为不存在的文件创建数据块
    auto not_exist_file_chunk_handle_or =
        metadataManager_->CreateChunkHandle(not_exist_file, 0);
    EXPECT_TRUE(!not_exist_file_chunk_handle_or.ok());
    EXPECT_TRUE(IsNotFound(not_exist_file_chunk_handle_or.status()));

    const std::string not_exist_dir_file = "/not_exist_file/file";
    auto create_not_exist_dir_metadata =
        metadataManager_->CreateFileMetadata(not_exist_dir_file);
    EXPECT_TRUE(!create_not_exist_dir_metadata.ok());
    EXPECT_TRUE(IsNotFound(create_not_exist_dir_metadata));
}

// 多个线程创建多目录文件，如 /a/b/c/d/e/f/g/i
// 确保命名空间的正确性
TEST_F(MetadataManagerTest, CreateDirFileMetadataInParallel) {
    const int numOfThread = 50;
    // 生成多级文件名
    auto GetLevelName = [](int level) {
        std::string name = "";
        for (int i = 0; i < level; i++) {
            std::string cur = "/l" + std::to_string(i);
            name = name + cur;
        }
        return name;
    };

    std::vector<std::thread> threads;

    for (int i = 0; i < numOfThread; i++) {
        threads.push_back(std::thread([&, i]() {
            auto status = metadataManager_->CreateFileMetadata(GetLevelName(i));

            while (!status.ok()) {
                status = metadataManager_->CreateFileMetadata(GetLevelName(i));
            }
        }));
    }

    // 等待所有线程运行结束
    for (auto& thread: threads) {
        thread.join();
    }
    threads.clear();

    // 验证所有文件都已创建
    for (int i = 0; i < numOfThread; i++) {
        auto metadata_or = metadataManager_->GetFileMetadata(GetLevelName(i));
        EXPECT_TRUE(metadata_or.ok());
    }
}