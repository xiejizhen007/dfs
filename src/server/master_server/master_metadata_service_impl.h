#ifndef DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H
#define DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H

#include "master_metadata_service.grpc.pb.h"
#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/server/master_server/chunk_server_manager.h"
#include "src/server/master_server/metadata_manager.h"

namespace dfs {
namespace server {

class MasterMetadataServiceImpl final
    : public protos::grpc::MasterMetadataService::Service {
   public:
    MasterMetadataServiceImpl();

    std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>
    GetChunkServerFileServiceClient(const std::string& server_address);

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

    ChunkServerManager* chunk_server_manager();

    MetadataManager* metadata_manager();

    absl::flat_hash_map<
        std::string,
        std::shared_ptr<dfs::grpc_client::ChunkServerFileServiceClient>>
        chunk_server_file_service_clients_;

    absl::Mutex chunk_server_file_service_clients_lock_;
};

}  // namespace server
}  // namespace dfs

#endif  // DFS_SERVER_MASTER_METADATA_SERVICE_IMPL_H