#include "src/client/client_cache_manager.h"
#include "src/common/system_logger.h"

namespace dfs {
namespace client {

using google::protobuf::util::InvalidArgumentError;
using google::protobuf::util::NotFoundError;
using google::protobuf::util::OkStatus;

google::protobuf::util::Status CacheManager::SetChunkHandle(
    const std::string& filename, const uint32_t& chunk_index,
    const std::string& chunk_handle) {
    auto chunk_status_or = GetChunkHandle(filename, chunk_index);
    if (chunk_status_or.ok()) {
        if (chunk_status_or.value() != chunk_handle) {
            return InvalidArgumentError(
                "reassigning a chunk handle for " + filename +
                " at chunk index " + std::to_string(chunk_index) + " from " +
                chunk_status_or.value() + " to " + chunk_handle);
        }
        return OkStatus();
    }

    chunk_handles_[filename][chunk_index] = chunk_handle;
    valid_chunk_handles_.insert(chunk_handle);

    return OkStatus();
}

google::protobuf::util::StatusOr<std::string> CacheManager::GetChunkHandle(
    const std::string& filename, const uint32_t& chunk_index) const {
    LOG(INFO) << "GetChunkHanle, filename: " << filename << " chunk_index: " << chunk_index;
    if (chunk_handles_.empty()) {
        LOG(INFO) << "CacheManager is empty";
        return NotFoundError("CacheManager is empty");
    }

    if (!chunk_handles_.contains(filename)) {
        return NotFoundError("CacheManager not found filename");
    }

    LOG(INFO) << "GetChunkHanle check filename ok";

    if (!chunk_handles_.at(filename).contains(chunk_index)) {
        return NotFoundError("CacheManager not found chunk_index");
    }

    LOG(INFO) << "GetChunkHanle check chunk_index ok";

    return chunk_handles_.at(filename).at(chunk_index);
}

google::protobuf::util::Status CacheManager::SetChunkVersion(
    const std::string& chunk_handle, const uint32_t& version) {
    if (!valid_chunk_handles_.contains(chunk_handle)) {
        return NotFoundError("CacheManager chunk hanle not found " +
                             chunk_handle);
    }

    chunk_versions_[chunk_handle] = version;
    return OkStatus();
}

google::protobuf::util::StatusOr<uint32_t> CacheManager::GetChunkVersion(
    const std::string& chunk_handle) const {
    if (!chunk_versions_.contains(chunk_handle)) {
        return NotFoundError("CacheManager chunk hanle not found " +
                             chunk_handle);
    }

    return chunk_versions_.at(chunk_handle);
}

google::protobuf::util::Status CacheManager::SetChunkServerLocationEntry(
    const std::string& chunk_handle, const ChunkServerLocationEntry& entry) {
    if (!valid_chunk_handles_.contains(chunk_handle)) {
        return NotFoundError("chunk handle not found " + chunk_handle);
    }

    chunk_server_locations_[chunk_handle] = entry;
    return OkStatus();
}

google::protobuf::util::StatusOr<CacheManager::ChunkServerLocationEntry>
CacheManager::GetChunkServerLocationEntry(
    const std::string& chunk_handle) const {
    if (!chunk_server_locations_.contains(chunk_handle)) {
        return NotFoundError("chunk handle not found " + chunk_handle);
    }

    return chunk_server_locations_.at(chunk_handle);
}

}  // namespace client
}  // namespace dfs