#include "src/server/chunk_server/file_chunk_manager.h"

#include "chunk_server.pb.h"

namespace dfs {
namespace server {

FileChunkManager* FileChunkManager::GetInstance() {
    static FileChunkManager* instance = new FileChunkManager();
    return instance;
}

bool FileChunkManager::Initialize(const std::string& chunk_dbname,
                                  const uint32_t& max_bytes_per_chunk) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = max_bytes_per_chunk * 128;
    leveldb::Status status = leveldb::DB::Open(options, chunk_dbname, &db);
    if (!status.ok()) {
        return false;
    }

    chunk_db_ = std::unique_ptr<leveldb::DB>(db);
    max_bytes_per_chunk_ = max_bytes_per_chunk;
    return true;
}

google::protobuf::util::Status FileChunkManager::CreateChunk(
    const std::string& chunk_handle, const uint32_t& chunk_version) {
    std::unique_ptr<leveldb::Iterator> db_read_iter(
        chunk_db_->NewIterator(leveldb::ReadOptions()));

    // 快速查找
    db_read_iter->Seek(chunk_handle);

    // does the chunk already exist?
    if (db_read_iter->Valid() && db_read_iter->status().ok() &&
        db_read_iter->key() == chunk_handle) {
        return google::protobuf::util::AlreadyExistsError(
            "chunk already exist, chunk_handle: " + chunk_handle);
    }

    protos::FileChunk chunk;
    chunk.set_version(chunk_version);

    auto status = WriteFileChunk(chunk_handle, chunk);
    if (!status.ok()) {
        return google::protobuf::util::UnknownError("Create chunk failed: " +
                                                    status.ToString());
    }

    return google::protobuf::util::OkStatus();
}

google::protobuf::util::StatusOr<std::string> FileChunkManager::ReadFromChunk(
    const std::string& chunk_handle, const uint32_t& version,
    const uint32_t& offset, const uint32_t& length) {
    // get the specified verison of the chunk
    auto file_chunk_or = GetFileChunk(chunk_handle, version);
    if (!file_chunk_or.ok()) {
        return file_chunk_or.status();
    }

    auto file_chunk = file_chunk_or.value();

    // out of range?
    if (offset > file_chunk->data().size()) {
        return google::protobuf::util::OutOfRangeError(
            "out of range when read chunk: " + chunk_handle);
    }

    return file_chunk->data().substr(offset, length);
}

google::protobuf::util::StatusOr<uint32_t> FileChunkManager::WriteToChunk(
    const std::string& chunk_handle, const uint32_t& version,
    const uint32_t& offset, const uint32_t& length, const std::string& data) {
    // get the specified verison of the chunk
    auto file_chunk_or = GetFileChunk(chunk_handle, version);
    if (!file_chunk_or.ok()) {
        return file_chunk_or.status();
    }

    auto file_chunk = file_chunk_or.value();

    // out of range?
    if (offset > file_chunk->data().size()) {
        return google::protobuf::util::OutOfRangeError(
            "out of range when write chunk: " + chunk_handle);
    }

    // TODO: data is too big? stop writing
    // 当前 chunk 可用的字节数
    uint32_t remaining_bytes = max_bytes_per_chunk_ - offset;
    if (!remaining_bytes) {
        return google::protobuf::util::OutOfRangeError(
            "chunk is full when write chunk: " + chunk_handle);
    }

    // 实际写入的字节数
    uint32_t write_length = std::min(remaining_bytes, length);
    file_chunk->mutable_data()->replace(offset, write_length, data);

    auto status = WriteFileChunk(chunk_handle, *file_chunk);
    if (!status.ok()) {
        return google::protobuf::util::UnknownError(
            "failed to write file chunk to db, chunk_handle: " + chunk_handle +
            " status: " + status.ToString());
    }

    return write_length;
}

google::protobuf::util::StatusOr<uint32_t> FileChunkManager::AppendToChunk(
    const std::string& chunk_handle, const uint32_t& version,
    const uint32_t& length, const std::string& data) {
    // get the specified verison of the chunk
    auto file_chunk_or = GetFileChunk(chunk_handle, version);
    if (!file_chunk_or.ok()) {
        return file_chunk_or.status();
    }

    auto file_chunk = file_chunk_or.value();
    // 将 offset 设置为 chunk 的末尾
    uint32_t offset = file_chunk->data().size();
    uint32_t remaining_bytes = max_bytes_per_chunk_ - offset;
    if (!remaining_bytes) {
        return google::protobuf::util::OutOfRangeError(
            "chunk is full when write chunk: " + chunk_handle);
    }

    // 实际写入的长度
    uint32_t append_length = std::min(remaining_bytes, length);
    file_chunk->set_data(file_chunk->data() + data.substr(append_length));

    auto status = WriteFileChunk(chunk_handle, *file_chunk);
    if (!status.ok()) {
        return google::protobuf::util::UnknownError(
            "failed to append data to chunk, chunk_handle: " + chunk_handle +
            " status: " + status.ToString());
    }

    return append_length;
}

google::protobuf::util::Status FileChunkManager::DeleteChunk(
    const std::string& chunk_handle) {
    leveldb::WriteOptions options;
    options.sync = true;

    auto status = chunk_db_->Delete(options, chunk_handle);
    if (!status.ok()) {
        return google::protobuf::util::UnknownError(
            "failed to delete file chunk, handle: " + chunk_handle +
            " status: " + status.ToString());
    }

    return google::protobuf::util::OkStatus();
}

leveldb::Status FileChunkManager::WriteFileChunk(
    const std::string& chunk_handle, const protos::FileChunk& chunk) {
    leveldb::WriteOptions options;
    // 开启同步
    options.sync = true;
    return chunk_db_->Put(options, chunk_handle, chunk.SerializeAsString());
}

std::list<protos::FileChunkMetadata>
FileChunkManager::GetAllFileChunkMetadata() {
    std::list<protos::FileChunkMetadata> metadatas;
    std::unique_ptr<leveldb::Iterator> it(
        chunk_db_->NewIterator(leveldb::ReadOptions()));

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        protos::FileChunk chunk;
        if (chunk.ParseFromString(it->value().ToString())) {
            protos::FileChunkMetadata metadata;
            metadata.set_chunk_handle(it->key().ToString());
            metadata.set_version(chunk.version());

            metadatas.emplace_back(metadata);
        }
    }

    return metadatas;
}

google::protobuf::util::Status FileChunkManager::UpdateChunkVersion(
    const std::string& chunk_handle, const uint32_t& old_version,
    const uint32_t& new_version) {
    // get the specified verison of the chunk
    auto file_chunk_or = GetFileChunk(chunk_handle, old_version);
    if (!file_chunk_or.ok()) {
        return file_chunk_or.status();
    }

    auto file_chunk = file_chunk_or.value();
    file_chunk->set_version(new_version);

    // write change back to db
    auto status = WriteFileChunk(chunk_handle, *file_chunk.get());
    if (!status.ok()) {
        return google::protobuf::util::UnknownError("");
    }

    // update chunk_verisons in memory
    chunk_versions_.Set(chunk_handle, new_version);

    return google::protobuf::util::OkStatus();
}

google::protobuf::util::StatusOr<uint32_t> FileChunkManager::GetChunkVersion(
    const std::string& chunk_handle) {
    auto value_pair = chunk_versions_.TryGet(chunk_handle);
    if (!value_pair.second) {
        auto chunk_or = GetFileChunk(chunk_handle);
        if (!chunk_or.ok()) {
            return chunk_or.status();
        }

        uint32_t version = chunk_or.value()->version();
        chunk_versions_.Set(chunk_handle, version);
        return version;
    }

    return value_pair.first;
}

google::protobuf::util::StatusOr<std::shared_ptr<protos::FileChunk>>
FileChunkManager::GetFileChunk(const std::string& chunk_handle) {
    leveldb::ReadOptions options;
    std::string data;

    auto status = chunk_db_->Get(options, chunk_handle, &data);
    if (!status.ok()) {
        return google::protobuf::util::NotFoundError(
            "chunk not found, chunk_handle: " + chunk_handle +
            ", status: " + status.ToString());
    }

    std::shared_ptr<protos::FileChunk> chunk(new protos::FileChunk);
    if (!chunk->ParseFromString(data)) {
        return google::protobuf::util::InternalError(
            "chunk parse failed, data: " + data);
    }

    return chunk;
}

google::protobuf::util::StatusOr<std::shared_ptr<protos::FileChunk>>
FileChunkManager::GetFileChunk(const std::string& chunk_handle,
                               const uint32_t& version) {
    auto chunk_or = GetFileChunk(chunk_handle);
    if (!chunk_or.ok()) {
        return chunk_or.status();
    }

    if (chunk_or.value()->version() != version) {
        return google::protobuf::util::NotFoundError(
            "no chunk found for the specified version, handle: " +
            chunk_handle + " version: " + std::to_string(version));
    }

    return chunk_or.value();
}

}  // namespace server
}  // namespace dfs