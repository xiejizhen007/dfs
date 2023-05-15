#include "src/client/dfs_client.h"

namespace dfs {
namespace client {

using google::protobuf::util::OkStatus;

google::protobuf::util::Status open(const std::string& filename,
                                    unsigned int flag) {
    return OkStatus();
}

google::protobuf::util::StatusOr<Data> read(const std::string& filename,
                                            size_t offset, size_t nbytes) {
    return OkStatus();
}

google::protobuf::util::Status write(const std::string& filename,
                                     const std::string& data, size_t offset,
                                     size_t nbytes) {
    return OkStatus();
}

google::protobuf::util::Status remove(const std::string& filename) {
    return OkStatus();
}

google::protobuf::util::Status close(const std::string& filename) {
    return OkStatus();
}

}  // namespace client
}  // namespace dfs