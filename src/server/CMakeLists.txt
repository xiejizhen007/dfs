include_directories(${PROJECT_SOURCE_DIR})

aux_source_directory(${PROJECT_SOURCE_DIR}/src/server/chunk_server chunk_server_sources)
aux_source_directory(${PROJECT_SOURCE_DIR}/src/server/master_server master_server_sources)

add_executable(chunk_server ${chunk_server_sources})
add_executable(master_server ${master_server_sources})

target_link_libraries(chunk_server
    PRIVATE
        protos_shared
        common_shared
)

target_link_libraries(master_server
    PRIVATE
        protos_shared
        common_shared
)