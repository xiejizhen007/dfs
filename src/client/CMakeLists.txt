include_directories(${PROJECT_SOURCE_DIR})

aux_source_directory(${PROJECT_SOURCE_DIR}/src/client client_sources)

add_executable(dfs_client ${client_sources})

target_link_libraries(dfs_client
    PRIVATE
        protos_shared
        common_shared
        grpc_client_shared
)