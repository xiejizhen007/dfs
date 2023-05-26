#include "src/server/chunk_server/chunk_server_impl.h"

#include <absl/time/time.h>

#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

namespace dfs {
namespace server {

using dfs::grpc_client::ChunkServerFileServiceClient;
using dfs::grpc_client::ChunkServerManagerServiceClient;
using protos::grpc::ReportChunkServerRequest;

ChunkServerImpl* ChunkServerImpl::GetInstance() {
    static ChunkServerImpl* instance = new ChunkServerImpl();
    return instance;
}

ChunkServerImpl::~ChunkServerImpl() {}

bool ChunkServerImpl::Initialize(const std::string& server_name,
                                 dfs::common::ConfigManager* config_manager) {
    config_manager_ = config_manager;
    chunk_server_name_ = server_name;
    server_address_ = config_manager_->GetChunkServerAddress(server_name);
    server_port_ = config_manager_->GetChunkServerPort(server_name);
    return true;
}

bool ChunkServerImpl::ReportToMaster() {
    ReportChunkServerRequest request;

    auto chunk_server = request.mutable_chunk_server();
    auto chunk_server_location = chunk_server->mutable_location();
    chunk_server_location->set_server_hostname(server_address_);
    chunk_server_location->set_server_port(server_port_);

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
        LOG(ERROR) << "master server client not responding " << respond.status();
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

// 获取块句柄对应的版本号
google::protobuf::util::StatusOr<uint32_t> ChunkServerImpl::GetChunkVersion(
    const std::string& chunk_handle) {
    return FileChunkManager::GetInstance()->GetChunkVersion(chunk_handle);
}

void ChunkServerImpl::AddOrUpdateLease(const std::string& chunk_handle,
                                       const uint64_t& expiration_unix_sec) {
    absl::WriterMutexLock lease_unix_sec_lock_guard(&lease_unix_sec_lock_);
    lease_unix_sec_[chunk_handle] = expiration_unix_sec;
}

void ChunkServerImpl::RemoveLease(const std::string& chunk_handle) {
    absl::WriterMutexLock lease_unix_sec_lock_guard(&lease_unix_sec_lock_);
    lease_unix_sec_.erase(chunk_handle);
}

bool ChunkServerImpl::HasWriteLease(const std::string& chunk_handle) {
    absl::ReaderMutexLock lease_unix_sec_lock_guard(&lease_unix_sec_lock_);
    if (!lease_unix_sec_.contains(chunk_handle)) {
        return false;
    }

    return absl::Now() < absl::FromUnixSeconds(lease_unix_sec_[chunk_handle]);
}

std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
ChunkServerImpl::GetOrCreateChunkServerFileServerClient(
    const std::string& server_address) {
    {
        absl::ReaderMutexLock chunk_server_file_server_clients_lock_guard(
            &chunk_server_file_server_clients_lock_);
        if (chunk_server_file_server_clients_.contains(server_address)) {
            return chunk_server_file_server_clients_[server_address];
        }
    }

    absl::WriterMutexLock chunk_server_file_server_clients_lock_guard(
        &chunk_server_file_server_clients_lock_);
    // create a new client
    chunk_server_file_server_clients_[server_address] =
        std::make_shared<ChunkServerFileServiceClient>(grpc::CreateChannel(
            server_address, grpc::InsecureChannelCredentials()));

    return chunk_server_file_server_clients_[server_address];
}

std::string ChunkServerImpl::GetChunkServerLocation() const {
    return server_address_ + ":" + std::to_string(server_port_);
}

}  // namespace server
}  // namespace dfs