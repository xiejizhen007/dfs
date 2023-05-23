#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include "src/client/dfs_client.h"

using google::protobuf::util::IsAlreadyExists;

static void BM_OPEN_WITH_MODE_CREATE(benchmark::State& state) {
    auto init_status = dfs::client::init_client();
    uint64_t ok = 0;
    uint64_t failed = 0;
    for (auto _ : state) {
        auto status = dfs::client::open("/open_with_mode_create", dfs::client::OpenFlag::CREATE);
        state.PauseTiming();
        if (status.ok() || IsAlreadyExists(status)) {
            ok++;
        } else {
            failed++;
        }
        state.ResumeTiming();
    }
    state.counters["ok"] = ok;
    state.counters["failed"] = failed;
}

static void BM_READ_FILE(benchmark::State& state) {
    auto init_status = dfs::client::init_client();
    uint64_t ok = 0;
    uint64_t failed = 0;
    for (auto _ : state) {
        auto status_or = dfs::client::read("/benchmarks_read", 0, 5);
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

BENCHMARK(BM_OPEN_WITH_MODE_CREATE);
BENCHMARK(BM_READ_FILE);

// 运行基准测试
BENCHMARK_MAIN();