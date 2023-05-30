#include <absl/strings/str_split.h>

#include <iostream>
#include <string>

#include "src/client/dfs_client.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using namespace dfs::client;
using dfs::common::ConfigManager;

std::vector<std::string> ParseCommand(const std::string& command) {
    std::vector<std::string> token = absl::StrSplit(command, ' ');
    return token;
}

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);

    const std::string config_path =
        std::string(CMAKE_SOURCE_DIR) + "/config.json";

    if (!ConfigManager::GetInstance()->InitConfigManager(config_path)) {
        LOG(ERROR) << "config init error, check config path";
        return 1;
    }

    init_client();
    std::string command;

    while (true) {
        std::cout << "DFS >> ";
        std::getline(std::cin, command);

        const auto& token = ParseCommand(command);

        if (token[0] == "open" && token.size() == 3) {
            auto status = open(token[1].c_str(), std::stoi(token[2]));
            LOG(INFO) << "open status: " << status.ToString();
        } else if (token[0] == "read" && token.size() == 4) {
            auto status = read(token[1].c_str(), std::stoi(token[2]),
                               std::stoi(token[3]));
            if (status.ok()) {
                auto data = status.value();
                // LOG(INFO) << "read nbytes: " << data.bytes
                //           << ", buffer: " << data.buffer;
                LOG(INFO) << "nbytes: " << data.bytes;
            } else {
                LOG(INFO) << "read status: " << status.status().ToString();
            }
        } else if (token[0] == "write" && token.size() == 5) {
            auto status = write(token[1].c_str(), (void*)token[2].c_str(),
                                std::stoi(token[3]), std::stoi(token[4]));
            LOG(INFO) << "write status: " << status.status().ToString();
        } else if (token[0] == "remove" && token.size() == 2) {
            auto status = dfs::client::remove(token[1].c_str());
            LOG(INFO) << "remove status: " << status.ToString();
        } else if (token[0] == "upload" && token.size() == 2) {
            auto status = upload(token[1].c_str());
            LOG(INFO) << "upload status: " << status.ToString();
        } else if (token[0] == "set" && token.size() == 3) {
            auto status = set(token[1].c_str(), std::stoi(token[2]));
            LOG(INFO) << "set status: " << status.ToString();
        } else if (token[0] == "quit") {
            LOG(INFO) << "Quit....";
            break;
        } else {
            LOG(ERROR) << "unknow command";
        }
    }

    return 0;
}