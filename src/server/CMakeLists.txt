aux_source_directory(${PROJECT_SOURCE_DIR}/src/server server_sources)

add_executable(server ${server_sources})

target_link_libraries(server
    PRIVATE
        libproto
        common_shared
)