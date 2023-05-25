#include "src/server/master_server/master_metadata_service_impl.h"

#include <gtest/gtest.h>

#include "src/grpc_client/chunk_server_file_service_client.h"
#include "src/grpc_client/master_metadata_service_client.h"
#include "src/server/chunk_server/chunk_server_file_service_impl.h"
#include "src/server/chunk_server/file_chunk_manager.h"

class MasterMetadataServiceTest : public ::testing::Test {
   protected:
};

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // std::thread server_thread = std::thread(BuildServer);
    // std::this_thread::sleep_for(std::chrono::seconds(3));
    // InitChunk();

    // Run tests
    int exit_code = RUN_ALL_TESTS();

    // // Clean up background server
    // pthread_cancel(server_thread.native_handle());
    // server_thread.join();

    return exit_code;
}