include_directories(${PROJECT_SOURCE_DIR})

aux_source_directory(${PROJECT_SOURCE_DIR}/src/grpc_client grpc_client_sources)

add_library(grpc_client_shared ${grpc_client_sources})
target_link_libraries(grpc_client_shared
    PRIVATE
        protos_shared
        common_shared
)