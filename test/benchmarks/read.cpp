#include <benchmark/benchmark.h>
#include <glog/logging.h>
#include <leveldb/db.h>

#include <chrono>
#include <iostream>

#include "src/client/dfs_client.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using google::protobuf::util::IsAlreadyExists;

static void BM_READ_FILE(benchmark::State& state) {
    auto init_status = dfs::client::init_client();
    dfs::client::open("/benchmark_read", dfs::client::OpenFlag::CREATE);
    dfs::client::set("/benchmark_read", 128 * 1024 * 1024);

    uint64_t ok = 0;
    uint64_t failed = 0;
    const int fileSizeMB = state.range(0);

    for (auto _ : state) {
        auto status_or = dfs::client::read("/benchmark_read", 0,
                                           state.range(0) * 1024 * 1024);

        state.PauseTiming();
        if (status_or.ok()) {
            ok++;
        } else {
            failed++;
        }
        state.ResumeTiming();
    }

    state.counters["ok"] = ok;
    state.counters["failed"] = failed;
}

BENCHMARK(BM_READ_FILE)->RangeMultiplier(2)->Range(1, 128)->Iterations(100);

int main(int argc, char** argv) {
    // 初始化配置
    dfs::common::ConfigManager::GetInstance()->InitConfigManager(
        std::string(CMAKE_SOURCE_DIR) + "/config.json");
    // 运行基准测试
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}