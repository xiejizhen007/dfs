#include "src/client/dfs_client_impl.h"

#include "src/common/system_logger.h"
#include "src/common/utils.h"

namespace dfs {
namespace client {

using dfs::grpc_client::ChunkServerFileServiceClient;
using dfs::grpc_client::MasterMetadataServiceClient;
using google::protobuf::util::OkStatus;
using google::protobuf::util::UnknownError;
using protos::grpc::DeleteFileRequest;
using protos::grpc::OpenFileRequest;
using protos::grpc::ReadFileChunkRequest;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::WriteFileChunkRequest;

DfsClientImpl::DfsClientImpl() {
    // TODO: read ip:port from config file.
    const std::string& target_str = "127.0.0.1:50050";
    auto channel =
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials());
    master_metadata_service_client_ =
        std::make_shared<MasterMetadataServiceClient>(channel);

    cache_manager_ = std::make_shared<CacheManager>();
}

google::protobuf::util::Status DfsClientImpl::CreateFile(
    const std::string& filename) {
    // set up request
    OpenFileRequest request;
    request.set_filename(filename);
    request.set_mode(OpenFileRequest::CREATE);
    // call rpc, create file.
    auto respond_or = master_metadata_service_client_->SendRequest(request);
    if (!respond_or.ok()) {
        return respond_or.status();
    }
    // cache file metadata.
    CacheToCacheManager(filename, 0, respond_or.value());
    return OkStatus();
}

google::protobuf::util::Status DfsClientImpl::DeleteFile(
    const std::string& filename) {
    // set up request
    DeleteFileRequest request;
    request.set_filename(filename);
    return master_metadata_service_client_->SendRequest(request);
}

google::protobuf::util::StatusOr<std::pair<size_t, std::string>>
DfsClientImpl::ReadFile(const std::string& filename, size_t offset,
                        size_t nbytes) {
    // 一个 chunk 4MB
    // TODO: use config file
    const size_t chunk_size = 4 * common::bytesMB;

    size_t bytes_read = 0;
    size_t remain_bytes = nbytes;
    // 在 chunk 的起始位置
    size_t chunk_start_offset = offset % chunk_size;

    void* buffer = malloc(nbytes);
    if (!buffer) {
        return UnknownError("malloc failed");
    }

    for (size_t chunk_index = offset / chunk_size; remain_bytes > 0;
         chunk_index++) {
        size_t bytes_to_read = std::min(remain_bytes, chunk_size);
        auto read_file_chunk_or = ReadFileChunk(
            filename, chunk_index, chunk_start_offset, bytes_to_read);
        if (!read_file_chunk_or.ok()) {
            free(buffer);
            return read_file_chunk_or.status();
        }

        // copy data
        size_t actually_read_bytes = read_file_chunk_or.value().read_length();
        const void* actually_read_data =
            read_file_chunk_or.value().data().c_str();
        memmove((char*)buffer + bytes_read, actually_read_data,
                actually_read_bytes);

        // set parameters
        chunk_start_offset = 0;
        bytes_read += actually_read_bytes;
        remain_bytes -= actually_read_bytes;
    }

    auto res = std::string((char*)buffer, nbytes);
    free(buffer);
    return std::make_pair(bytes_read, res);
}

google::protobuf::util::StatusOr<protos::grpc::ReadFileChunkRespond>
DfsClientImpl::ReadFileChunk(const std::string& filename, size_t chunk_index,
                             size_t offset, size_t nbytes) {
    // TODO: set up chunk_handle, chunk_version from chunk metadata.
    std::string chunk_handle;
    size_t chunk_version;
    CacheManager::ChunkServerLocationEntry entry;
    // talk to master or cache
    auto status = GetChunkMetedata(filename, chunk_index, OpenFileRequest::READ,
                                   chunk_handle, chunk_version, entry);
    if (!status.ok()) {
        return status;
    }

    ReadFileChunkRequest request;
    request.set_chunk_handle(chunk_handle);
    request.set_version(chunk_version);
    request.set_offset(offset);
    request.set_length(nbytes);

    // auto location = entry.primary_location;
    // std::string server_address = location.server_hostname() + ":" +
    //                              std::to_string(location.server_port());

    // LOG(INFO) << "try to talk to chunkserver " << server_address;

    // // talk to chunkserver
    // auto chunk_server_file_service_client =
    //     GetChunkServerFileServiceClient(server_address);

    // auto respond_or = chunk_server_file_service_client->SendRequest(request);
    // if (!respond_or.ok()) {
    //     return respond_or.status();
    // }

    // return respond_or.value();
    // TODO: 轮询每个 chunkserver，只要有一个成功返回，立马退出
    for (const auto& location : entry.locations) {
        const std::string server_address =
            location.server_hostname() + ":" +
            std::to_string(location.server_port());

        LOG(INFO) << "try to talk to chunkserver " << server_address;

        auto chunk_server_file_service_client =
            GetChunkServerFileServiceClient(server_address);

        auto respond_or =
            chunk_server_file_service_client->SendRequest(request);
        if (respond_or.ok()) {
            return respond_or.value();
        }
    }

    return google::protobuf::util::UnknownError(
        "cant not read from entry.location");
}

google::protobuf::util::StatusOr<size_t> DfsClientImpl::WriteFile(
    const std::string& filename, const std::string& data, size_t offset,
    size_t nbytes) {
    // TODO: use config file, chunk is 4MB
    const size_t chunk_size = 4 * common::bytesMB;
    size_t chunk_start_offset = offset % chunk_size;
    size_t remain_bytes = nbytes;
    size_t bytes_write = 0;

    for (size_t chunk_index = offset / chunk_size; remain_bytes > 0;
         chunk_index++) {
        size_t bytes_to_write = std::min(remain_bytes, chunk_size);

        auto respond_or =
            WriteFileChunk(filename, data.substr(bytes_write), chunk_index,
                           chunk_start_offset, bytes_to_write);
        if (!respond_or.ok()) {
            return respond_or.status();
        }

        size_t actually_write_bytes = respond_or.value().write_length();

        // set parameters
        chunk_start_offset = 0;
        bytes_write += actually_write_bytes;
        remain_bytes -= actually_write_bytes;
    }

    return bytes_write;
}

google::protobuf::util::StatusOr<protos::grpc::WriteFileChunkRespond>
DfsClientImpl::WriteFileChunk(const std::string& filename,
                              const std::string& data, size_t chunk_index,
                              size_t offset, size_t nbytes) {
    std::string chunk_handle;
    size_t chunk_version;
    CacheManager::ChunkServerLocationEntry entry;
    // talk to master or cache
    auto status = GetChunkMetedata(filename, chunk_index, OpenFileRequest::READ,
                                   chunk_handle, chunk_version, entry);
    if (!status.ok()) {
        return status;
    }

    // get chunk server location, handle data write
    // TODO: retry when write failed
    auto location = entry.locations.at(0);
    std::string server_address = location.server_hostname() + ":" +
                                 std::to_string(location.server_port());

    auto data_to_send = data.substr(0, nbytes);
    // calc checksum
    auto checksum = dfs::common::ComputeHash(data_to_send);

    // talk to chunkserver
    auto chunk_server_file_service_client =
        GetChunkServerFileServiceClient(server_address);

    // send chunk data first
    SendChunkDataRequest send_request;
    send_request.set_checksum(checksum);
    send_request.set_data(data_to_send);
    auto send_respond_or =
        chunk_server_file_service_client->SendRequest(send_request);
    if (!send_respond_or.ok()) {
        LOG(ERROR) << "send chunk data is failed, because "
                   << send_respond_or.status().ToString();
        return send_respond_or.status();
    }

    // write data to chunk
    WriteFileChunkRequest request;
    request.mutable_header()->set_chunk_handle(chunk_handle);
    request.mutable_header()->set_version(chunk_version);
    request.mutable_header()->set_offset(offset);
    request.mutable_header()->set_length(nbytes);
    // request.mutable_header()->set_data(data);
    request.mutable_header()->set_checksum(checksum);

    request.mutable_locations()->Add(std::move(location));

    auto respond_or = chunk_server_file_service_client->SendRequest(request);
    if (!respond_or.ok()) {
        LOG(ERROR) << "write file chunk respond is not ok";
        return respond_or.status();
    }

    return respond_or.value();
}

std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
DfsClientImpl::GetChunkServerFileServiceClient(const std::string& address) {
    auto value_pair = chunk_server_file_service_clients_.TryGet(address);
    if (!value_pair.second) {
        RegisterChunkServerFileServiceClient(address);
        value_pair = chunk_server_file_service_clients_.TryGet(address);
    }

    return value_pair.first;
}

bool DfsClientImpl::RegisterChunkServerFileServiceClient(
    const std::string& address) {
    std::shared_ptr<ChunkServerFileServiceClient> file_service_client =
        std::make_shared<ChunkServerFileServiceClient>(
            grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    return chunk_server_file_service_clients_.TryInsert(address,
                                                        file_service_client);
}

void DfsClientImpl::CacheToCacheManager(
    const std::string& filename, const uint32_t& chunk_index,
    const protos::grpc::OpenFileRespond& respond) {
    const std::string& chunk_handle = respond.metadata().chunk_handle();

    auto set_chunk_handle_status =
        cache_manager_->SetChunkHandle(filename, chunk_index, chunk_handle);
    if (!set_chunk_handle_status.ok()) {
        // add log
        return;
    }

    auto chunk_version_or = cache_manager_->GetChunkVersion(chunk_handle);
    auto respond_version = respond.metadata().version();
    if (!chunk_version_or.ok() || respond_version > chunk_version_or.value()) {
        // no cache or find a higher version
        cache_manager_->SetChunkVersion(chunk_handle, respond_version);
    } else {
        // add log
    }

    CacheManager::ChunkServerLocationEntry entry;
    entry.primary_location = respond.metadata().primary_location();
    for (const auto& location : respond.metadata().locations()) {
        entry.locations.emplace_back(location);
    }
    cache_manager_->SetChunkServerLocationEntry(chunk_handle, entry);
}

google::protobuf::util::Status DfsClientImpl::GetChunkMetedata(
    const std::string& filename, const size_t& chunk_index,
    const OpenFileRequest::OpenMode& openmode, std::string& chunk_handle,
    size_t& chunk_version, CacheManager::ChunkServerLocationEntry& entry) {
    bool metedata_in_cache = true;
    // get chunk_handle, chunk_version, entry from cache.
    auto cache_chunk_handle_or =
        cache_manager_->GetChunkHandle(filename, chunk_index);
    if (cache_chunk_handle_or.ok()) {
        chunk_handle = cache_chunk_handle_or.value();
        auto cache_chunk_version_or =
            cache_manager_->GetChunkVersion(chunk_handle);
        if (cache_chunk_version_or.ok()) {
            chunk_version = cache_chunk_version_or.value();
            auto cache_chunk_server_location_or =
                cache_manager_->GetChunkServerLocationEntry(chunk_handle);
            if (cache_chunk_server_location_or.ok()) {
                LOG(INFO) << "file metadata in cache";
                entry = cache_chunk_server_location_or.value();
            } else {
                metedata_in_cache = false;
            }
        } else {
            metedata_in_cache = false;
        }
    } else {
        metedata_in_cache = false;
    }

    if (!metedata_in_cache) {
        LOG(INFO) << "talk to master metadata service";
        // talk to master_metadata_service
        OpenFileRequest request;
        request.set_filename(filename);
        request.set_chunk_index(chunk_index);
        request.set_mode(openmode);

        auto respond_or = master_metadata_service_client_->SendRequest(request);
        if (!respond_or.ok()) {
            return respond_or.status();
        }

        // cache
        CacheToCacheManager(filename, chunk_index, respond_or.value());
        chunk_handle =
            cache_manager_->GetChunkHandle(filename, chunk_index).value();
        chunk_version = cache_manager_->GetChunkVersion(chunk_handle).value();
        entry =
            cache_manager_->GetChunkServerLocationEntry(chunk_handle).value();
    }

    // no chunk server?
    if (entry.locations.empty()) {
        return UnknownError("chunk server is empty");
    }

    return OkStatus();
}

}  // namespace client
}  // namespace dfs