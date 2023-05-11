#ifndef DFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H
#define DFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H

#include <list>
#include <memory>
#include <string>

#include "chunk_server.pb.h"
#include "google/protobuf/stubs/status.h"
#include "google/protobuf/stubs/statusor.h"
#include "leveldb/db.h"
#include "metadata.pb.h"

namespace dfs {
namespace server {

// control the chunks locally on the chunkserver

class FileChunkManager {
   public:
    static FileChunkManager* GetInstance();

    bool Initialize(const std::string& chunk_dbname);

    // interacting with leveldb

    google::protobuf::util::Status CreateChunk(const std::string& chunk_handle,
                                               const uint32_t& chunk_version);

    google::protobuf::util::StatusOr<std::string> ReadFromChunk(
        const std::string& chunk_handle, const uint32_t& offset,
        const uint32_t& length);

    // 将指定长度的数据写入至指定 uuid chunk 偏移 offset 处。
    google::protobuf::util::StatusOr<uint32_t> WriteToChunk(
        const std::string& chunk_handle, const uint32_t& offset,
        const uint32_t& length, const std::string& data);

    google::protobuf::util::StatusOr<uint32_t> AppendToChunk(
        const std::string& chunk_handle, const uint32_t& length,
        const std::string& data);

    google::protobuf::util::Status DeleteChunk(const std::string& chunk_handle);

    // write file chunk to leveldb
    leveldb::Status WriteFileChunk(const std::string& chunk_handle,
                                   const protos::FileChunk& chunk);

    // when chunkserver starts, report the stored chunkmetadata to master
    std::list<protos::FileChunkMetadata> GetAllFileChunkMetadata();

   private:
    FileChunkManager() = default;

    google::protobuf::util::StatusOr<std::shared_ptr<protos::FileChunk>>
    GetFileChunk(const std::string& chunk_handle);

    // chunk database
    std::unique_ptr<leveldb::DB> chunk_db_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_CHUNK_SERVER_FILE_CHUNK_MANAGER_H