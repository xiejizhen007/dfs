#ifndef DFS_SERVER_CHUNK_SERVER_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_IMPL_H

#include <atomic>
#include <string>
#include <thread>

#include "src/common/config_manager.h"
#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/grpc_client/chunk_server_manager_service_client.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace dfs {
namespace server {

class ChunkServerImpl {
   public:
    static ChunkServerImpl* GetInstance();

    bool Initialize(const std::string& server_name,
                    dfs::common::ConfigManager* config_manager);

    bool ReportToMaster();

    void StartReportToMaster();

    void StopReportToMaster();

    void RegisterMasterServerClient(const std::string& server_address);

    // 获取块句柄对应的版本号
    google::protobuf::util::StatusOr<uint32_t> GetChunkVersion(
        const std::string& chunk_handle);

    void AddOrUpdateLease(const std::string& chunk_handle,
                          const uint64_t& expiration_unix_sec);

    void RemoveLease(const std::string& chunk_handle);

    bool HasWriteLease(const std::string& chunk_handle);

    std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
    GetOrCreateChunkServerFileServerClient(const std::string& server_address);

   private:
    ChunkServerImpl() = default;

    ~ChunkServerImpl();

    std::string chunk_server_name_;
    dfs::common::ConfigManager* config_manager_;
    std::string server_address_;
    uint32_t server_port_;

    // A special thread, scheduled to execute ReportToMaster
    std::unique_ptr<std::thread> chunk_report_thread_;
    // to stop chunk_report_thread
    std::atomic<bool> stop_report_thread_{false};
    // use this to report, one master so one client.
    std::shared_ptr<dfs::grpc_client::ChunkServerManagerServiceClient>
        master_server_client_;

    // 租约
    absl::flat_hash_map<std::string, uint64_t> lease_unix_sec_;
    // 锁
    absl::Mutex lease_unix_sec_lock_;

    absl::flat_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>>
        chunk_server_file_server_clients_;

    absl::Mutex chunk_server_file_server_clients_lock_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_IMPL_H