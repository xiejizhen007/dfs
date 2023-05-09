#include "src/server/master_server/lock_manager.h"

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace dfs::server;

class LockManagerTest : public ::testing::Test {
   protected:
    void SetUp() override { lockManager_ = LockManager::GetInstance(); }

    LockManager* lockManager_;
};

TEST_F(LockManagerTest, AddLock) {
    EXPECT_EQ(lockManager_->ExistLock("/foo"), false);
    auto foo_lock_or = lockManager_->CreateLock("/foo");
    EXPECT_TRUE(foo_lock_or.ok());
    EXPECT_NE(foo_lock_or.value(), nullptr);

    auto bar_lock_or = lockManager_->CreateLock("/foo/bar");
    EXPECT_TRUE(bar_lock_or.ok());
    EXPECT_NE(bar_lock_or.value(), nullptr);
}

TEST_F(LockManagerTest, AddLockInParallel) {
    int numOfThreads = 10;
    std::vector<std::thread> threads;

    for (int i = 0; i < numOfThreads; i++) {
        threads.push_back(std::thread(
            [&, i]() { lockManager_->CreateLock("/" + std::to_string(i)); }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    threads.clear();

    for (int i = 0; i < numOfThreads; i++) {
        EXPECT_TRUE(lockManager_->ExistLock("/" + std::to_string(i)));
    }
}

TEST_F(LockManagerTest, AddSameLockInParallel) {
    std::atomic<int> count = 0;
    int numOfThreads = 10;
    std::vector<std::thread> threads;

    for (int i = 0; i < numOfThreads; i++) {
        threads.push_back(std::thread([&]() {
            auto status = lockManager_->CreateLock("/same");
            if (status.ok()) {
                count++;
            }
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    threads.clear();

    EXPECT_EQ(count, 1);
}

TEST_F(LockManagerTest, AcquireParentLocks) {
    auto a_lock_or = lockManager_->CreateLock("/a");
    EXPECT_NE(a_lock_or.value(), nullptr);
    auto b_lock_or = lockManager_->CreateLock("/a/b");
    EXPECT_NE(b_lock_or.value(), nullptr);
    auto c_lock_or = lockManager_->CreateLock("/a/b/c");
    EXPECT_NE(c_lock_or.value(), nullptr);

    ParentLocks plocks = ParentLocks(lockManager_, "/a/b/c");
    EXPECT_EQ(plocks.lock_size(), 2);
    EXPECT_TRUE(plocks.ok());
}

TEST_F(LockManagerTest, CheckErrorCases) {
    auto create_lock_or = lockManager_->CreateLock("/again");
    EXPECT_TRUE(create_lock_or.ok());
    auto create_again_lock_or = lockManager_->CreateLock("/again");
    EXPECT_TRUE(google::protobuf::util::IsAlreadyExists(create_again_lock_or.status()));

    auto non_exist_lock_or = lockManager_->FetchLock("/nonexist");
    EXPECT_EQ(non_exist_lock_or.ok(), false);
    EXPECT_TRUE(google::protobuf::util::IsNotFound(non_exist_lock_or.status()));

    ParentLocks plocks = ParentLocks(lockManager_, "/aa/bb/cc");
    EXPECT_EQ(plocks.lock_size(), 0);
    EXPECT_TRUE(google::protobuf::util::IsNotFound(plocks.status()));
}