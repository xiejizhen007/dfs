include_directories(${PROJECT_SOURCE_DIR})

aux_source_directory(${PROJECT_SOURCE_DIR}/src/common common_sources)
add_library(common_shared ${common_sources})

find_package(glog REQUIRED)
message(STATUS "glog version: ${glog_VERSION}")

target_link_libraries(common_shared
    glog
    jsoncpp
)

add_subdirectory(client)
add_subdirectory(protos)
add_subdirectory(server)
add_subdirectory(grpc_client)