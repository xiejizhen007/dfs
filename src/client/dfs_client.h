#ifndef DFS_CLIENT_DFS_CLIENT_H
#define DFS_CLIENT_DFS_CLIENT_H

#include <google/protobuf/stubs/statusor.h>

#include <string>

namespace dfs {
namespace client {

enum OpenFlag { READ = 1, WRITE = 2, CREATE = 4 };

struct Data {
    size_t bytes;
    std::string buffer;
    Data() : bytes(0), buffer(std::string()) {}
    Data(size_t _bytes, const std::string& _buffer)
        : bytes(_bytes), buffer(_buffer) {}
};

google::protobuf::util::Status open(const std::string& filename,
                                    unsigned int flag);

google::protobuf::util::StatusOr<Data> read(const std::string& filename,
                                            size_t offset, size_t nbytes);

google::protobuf::util::Status write(const std::string& filename,
                                     const std::string& data, size_t offset,
                                     size_t nbytes);

google::protobuf::util::Status remove(const std::string& filename);

google::protobuf::util::Status close(const std::string& filename);

}  // namespace client
}  // namespace dfs

#endif  // DFS_CLIENT_DFS_CLIENT_H