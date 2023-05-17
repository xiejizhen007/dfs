#include <iostream>
#include <string>

#include "src/client/dfs_client.h"
#include "src/common/system_logger.h"
#include <absl/strings/str_split.h>

using namespace dfs::client;

std::vector<std::string> ParseCommand(const std::string& command) {
    std::vector<std::string> token = absl::StrSplit(command, ' ');
    return token;
}

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
    init_client();
    std::string command;

    while (true) {
        std::cout << "DFS >> ";
        std::getline(std::cin, command);

        const auto& token = ParseCommand(command);

        if (token[0] == "open" && token.size() == 3) {
            auto status = open(token[1], std::stoi(token[2]));
            LOG(INFO) << "open status: " << status.ToString();
        } else if (token[0] == "read" && token.size() == 4) {
            auto status = read(token[1], std::stoi(token[2]), std::stoi(token[3]));
            LOG(INFO) << "read status: " << status.status().ToString();
        } else if (token[0] == "write" && token.size() == 5) {
            auto status = write(token[1], token[2], std::stoi(token[3]), std::stoi(token[4]));
            LOG(INFO) << "write status: " << status.status().ToString();
        } else if (token[0] == "remove" && token.size() == 2) {
            auto status = remove(token[1]);
            LOG(INFO) << "remove status: " << status.ToString();
        } else if (token[0] == "quit") {
            LOG(INFO) << "Quit....";
            break;
        } else {
            LOG(ERROR) << "unknow command";
        }

    }

    return 0;
}