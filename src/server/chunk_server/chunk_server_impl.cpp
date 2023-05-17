#include "src/server/chunk_server/chunk_server_impl.h"

#include <absl/time/time.h>

#include "src/common/system_logger.h"

namespace dfs {
namespace server {

using dfs::grpc_client::ChunkServerManagerServiceClient;
using protos::grpc::ReportChunkServerRequest;

ChunkServerImpl* ChunkServerImpl::GetInstance() {
    static ChunkServerImpl* instance = new ChunkServerImpl();
    return instance;
}

ChunkServerImpl::~ChunkServerImpl() {}

bool ChunkServerImpl::Initialize() {
    return true;
}

bool ChunkServerImpl::ReportToMaster() {
    ReportChunkServerRequest request;

    auto chunk_server = request.mutable_chunk_server();
    auto chunk_server_location = chunk_server->mutable_location();
    chunk_server_location->set_server_hostname("127.0.0.1");
    chunk_server_location->set_server_port(50100);

    auto all_chunk_data =
        FileChunkManager::GetInstance()->GetAllFileChunkMetadata();

    for (const auto& metadata : all_chunk_data) {
        //
        chunk_server->add_stored_chunk_handles(metadata.chunk_handle());
        LOG(INFO) << "store chunk handle: " << metadata.chunk_handle();
    }

    // report to master
    auto respond = master_server_client_->SendRequest(request);
    if (respond.ok()) {
        // ok, handle respond
        auto delete_chunk_handles = respond.value().delete_chunk_handles();
        for (const auto& chunk_handle : delete_chunk_handles) {
            LOG(INFO) << "start delete chunk handle: " << chunk_handle;
            auto status =
                FileChunkManager::GetInstance()->DeleteChunk(chunk_handle);
            if (!status.ok()) {
                LOG(ERROR) << "delete chunk handle: " << chunk_handle
                           << " failed, because " + status.ToString();
            }
        }
    } else {
        // handle error
        LOG(ERROR) << "master server client not responding";
        return false;
    }

    return true;
}

void ChunkServerImpl::StartReportToMaster() {
    chunk_report_thread_ = std::make_unique<std::thread>(std::thread([&]() {
        while (!stop_report_thread_.load()) {
            // report to master
            if (!ReportToMaster()) {
                LOG(ERROR) << "failed to report master server";
            }

            LOG(INFO) << "report to master";

            // TODO: use config file
            int64_t sleep_sec = 10;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
        }
    }));
}

void ChunkServerImpl::StopReportToMaster() {
    stop_report_thread_.store(true);
    chunk_report_thread_->join();
}

void ChunkServerImpl::RegisterMasterServerClient(
    const std::string& server_address) {
    if (!master_server_client_) {
        master_server_client_ =
            std::make_shared<ChunkServerManagerServiceClient>(
                grpc::CreateChannel(server_address,
                                    grpc::InsecureChannelCredentials()));
    }
}

}  // namespace server
}  // namespace dfs