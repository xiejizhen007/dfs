#include "src/common/system_logger.h"

namespace dfs {
namespace common {

void SystemLogger::Initialize(const std::string& program_name) {
    if (this->is_initialized_) {
        return;
    }

    // log to console
    FLAGS_logtostderr = true;

    google::InitGoogleLogging(program_name.c_str());
    google::InstallFailureSignalHandler();

    this->is_initialized_ = true;
}

}  // namespace common
}  // namespace dfs