#include "src/common/system_logger.h"

int main(int argc, char* argv[]) {
    dfs::common::SystemLogger::GetInstance().Initialize(argv[0]);
    LOG(INFO) << "hello";
    return 0;
}