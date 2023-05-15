#ifndef DFS_CLIENT_DFS_CLIENT_IMPL_H
#define DFS_CLIENT_DFS_CLIENT_IMPL_H

#include "master_metadata_service.grpc.pb.h"
#include "src/grpc_client/master_metadata_service_client.h"

#include <google/protobuf/stubs/statusor.h>

#include <string>

namespace dfs {
namespace client {

// 实现 client 对文件的基本操作
class DfsClientImpl {
   public:
    DfsClientImpl();

    google::protobuf::util::Status CreateFile(const std::string& filename);

    google::protobuf::util::Status DeleteFile(const std::string& filename);

   private:
    std::shared_ptr<dfs::grpc_client::MasterMetadataServiceClient> master_metadata_service_client_;
};

}  // namespace client
}  // namespace dfs

#endif  // DFS_CLIENT_DFS_CLIENT_IMPL_H