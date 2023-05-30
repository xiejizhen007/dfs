#include "src/server/chunk_server/file_chunk_manager.h"

#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <leveldb/db.h>

#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using dfs::server::FileChunkManager;

const int chunk_block_size = 64 * 1024 * 1024;

static void BM_GET_FILE_CHUNK(benchmark::State& state) {
    const std::string chunk_handle = "bm_get_file_chunk";
    FileChunkManager::GetInstance()->CreateChunk(chunk_handle, 1);
    const std::string data = std::string(chunk_block_size, '0');
    FileChunkManager::GetInstance()->WriteToChunk(chunk_handle, 1, 0,
                                                  data.size(), data);

    for (auto _ : state) {
        FileChunkManager::GetInstance()->GetFileChunk(chunk_handle);
    }
}

static void BM_GET_FILE_CHUNK_VALUE(benchmark::State& state) {
    const std::string chunk_handle = "bm_get_file_chunk_value";
    FileChunkManager::GetInstance()->CreateChunk(chunk_handle, 1);
    const std::string data = std::string(chunk_block_size, '0');
    FileChunkManager::GetInstance()->WriteToChunk(chunk_handle, 1, 0,
                                                  data.size(), data);

    for (auto _ : state) {
        auto file_chunk_or = FileChunkManager::GetInstance()->GetFileChunk(chunk_handle);
        if (!file_chunk_or.ok()) {
            continue;
        }

        auto file_chunk = file_chunk_or.value();
    }
}

static void BM_FILE_CHUNK_REPLACE(benchmark::State& state) {
    const std::string chunk_handle = "bm_file_chunk_replace";
    FileChunkManager::GetInstance()->CreateChunk(chunk_handle, 1);
    const std::string data = std::string(chunk_block_size, '0');
    FileChunkManager::GetInstance()->WriteToChunk(chunk_handle, 1, 0,
                                                  data.size(), data);

    for (auto _ : state) {
        auto file_chunk_or = FileChunkManager::GetInstance()->GetFileChunk(chunk_handle);
        if (!file_chunk_or.ok()) {
            continue;
        }

        auto file_chunk = file_chunk_or.value();
        file_chunk->mutable_data()->replace(0, chunk_block_size, data);
    }
}

static void BM_WRITE_TO_CHUNK(benchmark::State& state) {
    const std::string chunk_handle = "bm_write_to_chunk";
    FileChunkManager::GetInstance()->CreateChunk(chunk_handle, 1);
    const std::string data = std::string(chunk_block_size, '0');

    for (auto _ : state) {
        FileChunkManager::GetInstance()->WriteToChunk(chunk_handle, 1, 0,
                                                      data.size(), data);
    }
}

static void BM_WRITE_FILE_CHUNK(benchmark::State& state) {
    const std::string chunk_handle = "bm_write_file_chunk";
    FileChunkManager::GetInstance()->CreateChunk(chunk_handle, 1);
    const std::string data = std::string(chunk_block_size, '0');

    protos::FileChunk chunk;
    chunk.set_version(1);
    chunk.set_data(data);

    for (auto _ : state) {
        FileChunkManager::GetInstance()->WriteFileChunk(chunk_handle, chunk);
    }
}

static void BM_CHUNK_SERIALIZE_AS_STRING(benchmark::State& state) {
    const std::string data = std::string(chunk_block_size, '0');

    protos::FileChunk chunk;
    chunk.set_version(1);
    chunk.set_data(data);

    for (auto _ : state) {
        chunk.SerializeAsString();
    }
}

static void BM_LEVELDB_WRITE(benchmark::State& state) {
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status =
        leveldb::DB::Open(options, "benchmarks_leveldb", &db);

    // 写入数据
    std::string key = "BM_LEVELDB_WRITE";
    const std::string data = std::string(chunk_block_size, '0');

    protos::FileChunk chunk;
    chunk.set_version(1);
    chunk.set_data(data);

    const std::string chunk_str = chunk.SerializeAsString();

    for (auto _ : state) {
        db->Put(leveldb::WriteOptions(), key, chunk_str);
    }

    delete db;
}

BENCHMARK(BM_WRITE_TO_CHUNK)->Iterations(100);
BENCHMARK(BM_GET_FILE_CHUNK)->Iterations(100);
BENCHMARK(BM_GET_FILE_CHUNK_VALUE)->Iterations(100);
BENCHMARK(BM_FILE_CHUNK_REPLACE)->Iterations(100);
BENCHMARK(BM_WRITE_FILE_CHUNK)->Iterations(100);
BENCHMARK(BM_CHUNK_SERIALIZE_AS_STRING)->Iterations(100);
BENCHMARK(BM_LEVELDB_WRITE)->Iterations(100);

int main(int argc, char** argv) {
    // 初始化配置
    dfs::common::ConfigManager::GetInstance()->InitConfigManager(
        std::string(CMAKE_SOURCE_DIR) + "/config.json");

    dfs::server::FileChunkManager::GetInstance()->Initialize(
        "benchmarks_file_chunk_manager_test", chunk_block_size);

    // 运行基准测试
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}