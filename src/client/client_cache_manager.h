#ifndef DFS_CLIENT_CLIENT_CACHE_MANAGER_H
#define DFS_CLIENT_CLIENT_CACHE_MANAGER_H

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/time/time.h>
#include <google/protobuf/stubs/statusor.h>

#include <string>
#include <vector>

#include "chunk_server.pb.h"

namespace dfs {
namespace client {

// 缓存从 master 获取的数据
class CacheManager {
   public:
    struct ChunkServerLocationEntry {
        protos::ChunkServerLocation primary_location;
        std::vector<protos::ChunkServerLocation> locations;
        absl::Time timepoint;

        ChunkServerLocationEntry() : timepoint(absl::Now()) {}
        ChunkServerLocationEntry(
            const protos::ChunkServerLocation& _primary_location,
            const std::vector<protos::ChunkServerLocation>& _locations)
            : timepoint(absl::Now()),
              primary_location(_primary_location),
              locations(_locations) {}
    };

    google::protobuf::util::Status SetChunkHandle(
        const std::string& filename, const uint32_t& chunk_index,
        const std::string& chunk_handle);

    google::protobuf::util::StatusOr<std::string> GetChunkHandle(
        const std::string& filename, const uint32_t& chunk_index) const;

    google::protobuf::util::Status SetChunkVersion(
        const std::string& chunk_handle, const uint32_t& version);

    google::protobuf::util::StatusOr<uint32_t> GetChunkVersion(
        const std::string& chunk_handle) const;

    google::protobuf::util::Status SetChunkServerLocationEntry(
        const std::string& chunk_handle, const ChunkServerLocationEntry& entry);

    google::protobuf::util::StatusOr<ChunkServerLocationEntry>
    GetChunkServerLocationEntry(const std::string& chunk_handle) const;

   private:
    // map<filename, <chunk_index, chunk_handle>>
    absl::flat_hash_map<std::string, absl::flat_hash_map<uint32_t, std::string>>
        chunk_handles_;

    absl::flat_hash_set<std::string> valid_chunk_handles_;

    // map<chunk_handle, version>
    absl::flat_hash_map<std::string, uint32_t> chunk_versions_;

    // map<chunk_handle, ChunkServerLocationEntry>
    // 映射 chunk_handle 所代表的块所在的 chunkserver location
    absl::flat_hash_map<std::string, ChunkServerLocationEntry>
        chunk_server_locations_;
};

}  // namespace client
}  // namespace dfs

#endif  // DFS_CLIENT_CLIENT_CACHE_MANAGER_H