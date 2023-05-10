#ifndef DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H
#define DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H

#include "master_metadata_service.grpc.pb.h"
#include "src/server/master_server/metadata_manager.h"

namespace dfs {
namespace server {

class MasterMetadataServiceImpl final
    : public protos::grpc::MasterMetadataService::Service {
   public:
    MasterMetadataServiceImpl();

   protected:
    grpc::Status HandleFileCreation(
        const protos::grpc::OpenFileRequest* request,
        protos::grpc::OpenFileRespond* respond);

    grpc::Status HandleFileChunkCreation(
        const protos::grpc::OpenFileRequest* request,
        protos::grpc::OpenFileRespond* respond);

    grpc::Status HandleFileChunkRead(
        const protos::grpc::OpenFileRequest* request,
        protos::grpc::OpenFileRespond* respond);

    grpc::Status HandleFileChunkWrite(
        const protos::grpc::OpenFileRequest* request,
        protos::grpc::OpenFileRespond* respond);

    grpc::Status OpenFile(grpc::ServerContext* context,
                          const protos::grpc::OpenFileRequest* request,
                          protos::grpc::OpenFileRespond* respond) override;

    grpc::Status DeleteFile(grpc::ServerContext* context,
                            const protos::grpc::DeleteFileRequest* request,
                            google::protobuf::Empty* respond);

    MetadataManager* metadata_manager();
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H