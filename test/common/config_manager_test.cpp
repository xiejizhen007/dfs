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

    std::filesystem::path executablePath =
        std::filesystem::canonical("/proc/self/exe");
    std::string path = executablePath.parent_path().string() + "/config.json";

    std::cout << "Executable path: " << path << std::endl;
    EXPECT_TRUE(ConfigManager::GetInstance()->InitConfigManager(path));

    std::cout << "bin: " << CMAKE_SOURCE_DIR << std::endl;

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    auto chunk_servers = ConfigManager::GetInstance()->GetAllChunkServer();
    for (auto iter : chunk_servers) {
        std::cout << iter.first << " " << iter.second << std::endl;
    }

    return exit_code;
}