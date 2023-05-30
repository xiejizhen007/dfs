#include <thread>
#include <vector>

#include "src/client/dfs_client.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using dfs::common::ConfigManager;

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);

    const std::string config_path =
        std::string(CMAKE_SOURCE_DIR) + "/config.json";

    const int numOfThread = 3;

    if (!ConfigManager::GetInstance()->InitConfigManager(config_path)) {
        LOG(ERROR) << "config init error, check config path";
        return 1;
    }

    std::vector<std::thread> threads;

    auto start = std::chrono::high_resolution_clock::now();  // 记录开始时间
    for (int i = 0; i < numOfThread; i++) {
        threads.push_back(std::thread([&, i]() {
            dfs::client::init_client();
            const std::string filename = "/file" + std::to_string(i);

            dfs::client::open(filename.c_str(), dfs::client::OpenFlag::CREATE);

            dfs::client::set(filename.c_str(), 100 * 1024 * 1024);
        }));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();  // 记录结束时间
    double durationMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
            .count();

    LOG(INFO) << numOfThread << " thread write 1GB file, spend " << durationMs
              << "ms";
}