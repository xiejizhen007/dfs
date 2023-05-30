#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include "src/client/dfs_client.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using google::protobuf::util::IsAlreadyExists;

static void BM_WRITE_FILE(benchmark::State& state) {
    auto init_status = dfs::client::init_client();
    dfs::client::open("/benchmark_write", dfs::client::OpenFlag::CREATE);
    uint64_t ok = 0;
    uint64_t failed = 0;
    const int fileSizeMB = state.range(0);

    for (auto _ : state) {
        // 写入 fileSizeMB 数据
        auto status_or =
            dfs::client::set("/benchmark_write", fileSizeMB * 1024 * 1024);

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

BENCHMARK(BM_WRITE_FILE)->RangeMultiplier(2)->Range(1, 128)->Iterations(100);

int main(int argc, char** argv) {
    // 初始化配置
    dfs::common::ConfigManager::GetInstance()->InitConfigManager(
        std::string(CMAKE_SOURCE_DIR) + "/config.json");
    // 运行基准测试
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}