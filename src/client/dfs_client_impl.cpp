#include "src/client/dfs_client_impl.h"

#include <thread>
#include <vector>

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
using protos::grpc::ReadFileChunkRespond;
using protos::grpc::SendChunkDataRequest;
using protos::grpc::SendChunkDataRespond;
using protos::grpc::WriteFileChunkRequest;
using protos::grpc::WriteFileChunkRespond;

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
        if (!respond_or.ok()) {
            LOG(ERROR) << "read " << chunk_handle
                       << " error: " << respond_or.status().ToString();
            return respond_or.status();
        }

        auto respond = respond_or.value();
        switch (respond.status()) {
            case ReadFileChunkRespond::UNKNOW:
                LOG(ERROR) << "read file chunk respond unknow, chunk_handle: "
                           << chunk_handle;
                continue;
            case ReadFileChunkRespond::NOT_FOUND:
                LOG(ERROR) << "chunk not found: " << chunk_handle;
                continue;
            case ReadFileChunkRespond::OUT_OF_RANGE:
                LOG(ERROR) << "out of range when read " << chunk_handle;
                continue;
            case ReadFileChunkRespond::VERSION_ERROR:
                LOG(ERROR) << "version error when read chunk " << chunk_handle
                           << " version" << chunk_version;
                continue;
            default:
                break;
        }

        return respond;
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
    auto get_metadata_status =
        GetChunkMetedata(filename, chunk_index, OpenFileRequest::READ,
                         chunk_handle, chunk_version, entry);
    if (!get_metadata_status.ok()) {
        return get_metadata_status;
    }

    // 需要发送的数据
    auto data_to_send = data.substr(0, nbytes);
    // 数据的校验和
    auto checksum = dfs::common::ComputeHash(data_to_send);

    // 数据发送线程
    std::vector<std::thread> send_data_threads;
    for (const auto& location : entry.locations) {
        std::string server_address = location.server_hostname() + ":" +
                                     std::to_string(location.server_port());

        send_data_threads.push_back(std::thread([&, server_address]() {
            // 设置数据发送请求
            SendChunkDataRequest send_request;
            send_request.set_checksum(checksum);
            send_request.set_data(data_to_send);
            // 获取 grpc 客户端
            auto chunk_server_file_service_client =
                GetChunkServerFileServiceClient(server_address);
            auto send_respond_or =
                chunk_server_file_service_client->SendRequest(send_request);
            if (!send_respond_or.ok()) {
                LOG(ERROR) << "send chunk data is failed, because "
                           << send_respond_or.status().ToString();
            } else {
                // ok
                auto send_respond = send_respond_or.value();
                if (send_respond.status() ==
                    protos::grpc::SendChunkDataRespond::OK) {
                    LOG(INFO)
                        << "send chunk data to " << server_address << " is ok";
                } else {
                    LOG(ERROR) << "send chunk data to " << server_address
                               << " failed, because " << send_respond.status();
                }
            }
        }));
    }

    // 等待所有发送线程结束
    for (auto& thread : send_data_threads) {
        thread.join();
    }

    // TODO:
    // 将数据写入到主副本块服务器，让主副本块服务器在将更新推送到其他副本的块服务器

    // 将数据写入到所有的块服务器
    // std::vector<std::thread> write_threads;
    WriteFileChunkRespond respond;
    for (const auto& location : entry.locations) {
        std::string server_address = location.server_hostname() + ":" +
                                     std::to_string(location.server_port());
        // 设置写入请求
        WriteFileChunkRequest write_request;
        write_request.mutable_header()->set_chunk_handle(chunk_handle);
        write_request.mutable_header()->set_version(chunk_version);
        write_request.mutable_header()->set_offset(offset);
        write_request.mutable_header()->set_length(nbytes);
        write_request.mutable_header()->set_checksum(checksum);

        // 获取 grpc 客户端
        auto chunk_server_file_service_client =
            GetChunkServerFileServiceClient(server_address);
        auto respond_or =
            chunk_server_file_service_client->SendRequest(write_request);
        if (!respond_or.ok()) {
            LOG(ERROR) << "write file chunk respond is not ok, status: " << respond_or.status().ToString();
        }

        // TODO: 将写入请求发送给主副本块服务器，由主块服务器应用更改到其他副本服务器
        respond = respond_or.value();
    }

    return respond;
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