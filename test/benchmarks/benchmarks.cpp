#include <benchmark/benchmark.h>

// 定义要进行基准测试的函数
static void BM_StringConcatenation(benchmark::State& state) {
    std::string str1 = "Hello, ";
    std::string str2 = "world!";
    for (auto _ : state) {
        // 在循环中进行基准测试的操作
        std::string result = str1 + str2;
        benchmark::DoNotOptimize(result);
    }
}

// 定义要进行基准测试的函数
static void BM_Say(benchmark::State& state) {
    int x = 99555;
    int y = 1265;
    for (auto _ : state) {
        // 在循环中进行基准测试的操作
        int z = x + y * x;
    }
}

// 注册基准测试函数
BENCHMARK(BM_StringConcatenation);
BENCHMARK(BM_Say);

// 运行基准测试
BENCHMARK_MAIN();