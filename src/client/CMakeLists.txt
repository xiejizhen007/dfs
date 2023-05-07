aux_source_directory(${PROJECT_SOURCE_DIR}/src/client client_sources)

add_executable(client ${client_sources})

target_link_libraries(client
    PRIVATE
        libproto
        common_shared
)