#ifndef DFS_COMMON_UTILS_H
#define DFS_COMMON_UTILS_H

#include "google/protobuf/stubs/status.h"
#include "grpc++/grpc++.h"

namespace dfs {
namespace common {

google::protobuf::util::Status StatusGrpc2Protobuf(grpc::Status status);

grpc::Status StatusProtobuf2Grpc(google::protobuf::util::Status status);

}  // namespace common
}  // namespace dfs

google::protobuf::util::Status StatusGrpc2Protobuf(grpc::Status status);


#endif  // DFS_COMMON_UTILS_H