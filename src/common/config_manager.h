#ifndef DFS_COMMON_CONFIG_MANAGER_H
#define DFS_COMMON_CONFIG_MANAGER_H

#include <json/json.h>

#include <string>
#include <vector>

namespace dfs {
namespace common {

class ConfigManager {
   public:
    static ConfigManager* GetInstance();
    // 打开配置文件
    bool InitConfigManager(const std::string& path);

    uint32_t GetBlockSize() const;

    uint32_t GetGrpcTimeout() const;

    std::vector<std::pair<std::string, std::string>> GetAllMasterServer();

    std::vector<std::pair<std::string, std::string>> GetAllChunkServer();

    std::string GetChunkServerAddress(const std::string& server_name) const;

    uint32_t GetChunkServerPort(const std::string& server_name) const;

   private:
    ConfigManager() = default;
    Json::Value root_;
};

}  // namespace common
}  // namespace dfs

#endif  // DFS_COMMON_CONFIG_MANAGER_H