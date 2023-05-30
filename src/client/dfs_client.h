#ifndef DFS_CLIENT_DFS_CLIENT_H
#define DFS_CLIENT_DFS_CLIENT_H

#include <google/protobuf/stubs/statusor.h>

#include <string>

#include "src/client/dfs_client_impl.h"

namespace dfs {
namespace client {

enum OpenFlag { READ = 1, WRITE = 2, CREATE = 4 };

struct Data {
    size_t bytes;
    void* buffer;
    Data() : bytes(0), buffer(nullptr) {}
    Data(size_t _bytes, void* _buffer) : bytes(_bytes), buffer(_buffer) {}
};

google::protobuf::util::Status init_client();

google::protobuf::util::Status open(const char* filename, unsigned int flag);

google::protobuf::util::StatusOr<Data> read(const char* filename, size_t offset,
                                            size_t nbytes);

google::protobuf::util::StatusOr<size_t> write(const char* filename,
                                               void* buffer, size_t offset,
                                               size_t nbytes);

google::protobuf::util::Status remove(const char* filename);

google::protobuf::util::Status close(const char* filename);

google::protobuf::util::Status upload(const char* filename);

google::protobuf::util::Status set(const char* filename, size_t size);

void reset_client();

}  // namespace client
}  // namespace dfs

#endif  // DFS_CLIENT_DFS_CLIENT_H