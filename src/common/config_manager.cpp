#include "src/common/config_manager.h"

#include <fstream>
#include <iostream>

namespace dfs {
namespace common {

ConfigManager* ConfigManager::GetInstance() {
    static ConfigManager* instance = new ConfigManager();
    return instance;
}

bool ConfigManager::InitConfigManager(const std::string& path) {
    std::ifstream file;
    file.open(path);

    if (!file.is_open()) {
        return false;
    }

    file >> root_;
    file.close();
    return true;
}

uint32_t ConfigManager::GetBlockSize() const {
    return root_["disk"]["block_size"].asUInt();
}

uint32_t ConfigManager::GetGrpcTimeout() const {
    return root_["timeout"]["grpc"].asUInt();
}

std::vector<std::pair<std::string, std::string>>
ConfigManager::GetAllMasterServer() {
    std::vector<std::pair<std::string, std::string>> res;
    auto chunk_servers = root_["master_server"];
    for (int i = 0; i < chunk_servers.size(); i++) {
        std::string name = chunk_servers[i]["name"].asString();
        std::string address = chunk_servers[i]["address"].asString();
        uint32_t port = chunk_servers[i]["port"].asUInt();

        res.emplace_back(
            std::make_pair(name, address + ":" + std::to_string(port)));
    }
    return res;
}

std::vector<std::pair<std::string, std::string>>
ConfigManager::GetAllChunkServer() {
    std::vector<std::pair<std::string, std::string>> res;
    auto chunk_servers = root_["chunk_server"];
    for (int i = 0; i < chunk_servers.size(); i++) {
        std::string name = chunk_servers[i]["name"].asString();
        std::string address = chunk_servers[i]["address"].asString();
        uint32_t port = chunk_servers[i]["port"].asUInt();

        res.emplace_back(
            std::make_pair(name, address + ":" + std::to_string(port)));
    }
    return res;
}

std::string ConfigManager::GetChunkServerAddress(
    const std::string& server_name) const {
    // TODO: 找一个更优雅的方式
    auto chunk_servers = root_["chunk_server"];
    for (int i = 0; i < chunk_servers.size(); i++) {
        std::string name = chunk_servers[i]["name"].asString();
        if (name == server_name) {
            return chunk_servers[i]["address"].asString();
        }
    }

    return "";
}

uint32_t ConfigManager::GetChunkServerPort(
    const std::string& server_name) const {
    // TODO: 找一个更优雅的方式
    auto chunk_servers = root_["chunk_server"];
    for (int i = 0; i < chunk_servers.size(); i++) {
        std::string name = chunk_servers[i]["name"].asString();
        if (name == server_name) {
            return chunk_servers[i]["port"].asUInt();
        }
    }

    return 0;
}

}  // namespace common
}  // namespace dfs