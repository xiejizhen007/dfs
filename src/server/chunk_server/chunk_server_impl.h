#ifndef DFS_SERVER_CHUNK_SERVER_IMPL_H
#define DFS_SERVER_CHUNK_SERVER_IMPL_H

#include <atomic>
#include <string>
#include <thread>

#include "src/grpc_client/chunk_server_manager_service_client.h"
#include "src/server/chunk_server/file_chunk_manager.h"

namespace dfs {
namespace server {

class ChunkServerImpl {
   public:
    static ChunkServerImpl* GetInstance();

    bool Initialize();

    bool ReportToMaster();

    void StartReportToMaster();

    void StopReportToMaster();

    void RegisterMasterServerClient(const std::string& server_address);

   private:
    ChunkServerImpl() = default;

    ~ChunkServerImpl();

    std::string chunk_server_name_;

    // A special thread, scheduled to execute ReportToMaster
    std::unique_ptr<std::thread> chunk_report_thread_;
    // to stop chunk_report_thread
    std::atomic<bool> stop_report_thread_{false};
    // use this to report, one master so one client.
    std::shared_ptr<dfs::grpc_client::ChunkServerManagerServiceClient>
        master_server_client_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_IMPL_H