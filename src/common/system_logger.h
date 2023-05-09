#ifndef DFS_COMMON_SYSTEM_LOGGER_H
#define DFS_COMMON_SYSTEM_LOGGER_H

#include <glog/logging.h>

#include <string>

namespace dfs {
namespace common {

class SystemLogger {
public:
    static SystemLogger& GetInstance() {
        static SystemLogger instance;
        return instance;
    }

    SystemLogger(const SystemLogger&) = delete;

    void Initialize(const std::string& program_name);
private:
    bool is_initialized_;
    SystemLogger():is_initialized_(false) {}
};

}  // namespace common
}  // namespace dfs


#endif