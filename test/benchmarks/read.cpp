#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include "src/client/dfs_client.h"
#include "src/common/config_manager.h"
#include "src/common/system_logger.h"

using google::protobuf::util::IsAlreadyExists;

// static void BM_OPEN_WITH_MODE_CREATE(benchmark::State& state) {

//     for (auto _ : state) {
//         auto status = dfs::client::open("/open_with_mode_create",
//                                         dfs::client::OpenFlag::CREATE);
//         state.PauseTiming();
//         if (status.ok() || IsAlreadyExists(status)) {
//             ok++;
//         } else {
//             failed++;
//         }
//         state.ResumeTiming();
//     }
//     state.counters["ok"] = ok;
//     state.counters["failed"] = failed;
// }

static void BM_READ_FILE_IN_MUTIL_LENGTH(benchmark::State& state) {
    auto init_status = dfs::client::init_client();
    uint64_t ok = 0;
    uint64_t failed = 0;
    // 循环运行基准测试
    while (state.KeepRunning()) {
        // 读取指定大小的数据
        auto status_or = dfs::client::read("/file", 0, state.range(0));
        if (status_or.ok()) {
            ok++;
        } else {
            failed++;
        }
    }

    // 设置基准测试结果：每秒读取的数据量
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            state.range(0));
    state.counters["ok"] = ok;
    state.counters["failed"] = failed;
}

// BENCHMARK(BM_OPEN_WITH_MODE_CREATE);
// BENCHMARK(BM_READ_FILE);
BENCHMARK(BM_READ_FILE_IN_MUTIL_LENGTH)
    ->RangeMultiplier(2)
    ->Range(1024, 64 * 1024 * 1024 * 2);

int main(int argc, char** argv) {
    // 初始化配置
    dfs::common::ConfigManager::GetInstance()->InitConfigManager(
        std::string(CMAKE_SOURCE_DIR) + "/config.json");
    // 运行基准测试
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}