#include "src/common/config_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>

using dfs::common::ConfigManager;

class ConfigManagerTest : public ::testing::Test {};

TEST_F(ConfigManagerTest, OpenTest) {
    // 绝对路径可以运行，相对路径不行，需要修复
    EXPECT_EQ(ConfigManager::GetInstance()->GetBlockSize(), 64);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    std::cout << "bin: " << CMAKE_SOURCE_DIR << std::endl;
    const std::string config_path = std::string(CMAKE_SOURCE_DIR) + "/config.json";

    EXPECT_TRUE(ConfigManager::GetInstance()->InitConfigManager(config_path));

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    auto chunk_servers = ConfigManager::GetInstance()->GetAllChunkServer();
    for (auto iter : chunk_servers) {
        std::cout << iter.first << " " << iter.second << std::endl;
    }

    return exit_code;
}