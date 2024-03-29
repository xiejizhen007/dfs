# Add the Google Test library
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# find_package(absl REQUIRED)
# include_directories(${ABSL_INCLUDE_DIRS})
# message("absl version: ${absl_VERSION}")

# list(APPEND CMAKE_PREFIX_PATH /usr/local)

find_package(leveldb REQUIRED)
message("leveldb version: ${leveldb_VERSION}")

# if (leveldb_FOUND)
#     message("leveldb found")
#     message(STATUS "leveldb include dir: ${leveldb_INCLUDE_DIR}")
# endif()

include_directories(${PROJECT_SOURCE_DIR}/)

add_executable(config_manager_test
    common/config_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/common/config_manager.cpp
)

target_link_libraries(config_manager_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
    jsoncpp
)

add_executable(utils_test
    common/utils_test.cpp
    ${PROJECT_SOURCE_DIR}/src/common/utils.cpp
)

target_link_libraries(utils_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
)

add_executable(client_cache_manager_test
    client/client_cache_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/client/client_cache_manager.cpp
)

target_link_libraries(client_cache_manager_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
    glog
)

add_executable(lock_manager_test
    server/master_server/lock_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
)

target_link_libraries(lock_manager_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
)

add_executable(master_metadata_service_impl_test
    server/master_server/master_metadata_service_impl_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_file_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/master_metadata_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_server_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_replica_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/metadata_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
)

target_link_libraries(master_metadata_service_impl_test
    ${GTEST_BOTH_LIBRARIES}
    grpc_client_shared
    leveldb
    protos_shared
)

add_executable(metadata_manager_test
    server/master_server/metadata_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/metadata_manager.cpp
)

target_link_libraries(metadata_manager_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
)

add_executable(chunk_server_manager_test
    server/master_server/chunk_server_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_server_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_replica_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/metadata_manager.cpp
)

target_link_libraries(chunk_server_manager_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
    common_shared
    grpc_client_shared
)

add_executable(chunk_server_manager_service_impl_test
    server/master_server/chunk_server_manager_service_impl_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_server_manager_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_server_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/chunk_replica_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/metadata_manager.cpp
)

target_link_libraries(chunk_server_manager_service_impl_test
    ${GTEST_BOTH_LIBRARIES}
    protos_shared
    common_shared
    grpc_client_shared
)

add_executable(chunk_server_file_service_impl_test
    server/chunk_server/chunk_server_file_service_impl_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_file_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_impl.cpp
)

target_link_libraries(chunk_server_file_service_impl_test
    ${GTEST_BOTH_LIBRARIES}
    leveldb
    protos_shared
    grpc_client_shared
)

add_executable(chunk_server_lease_service_impl_test
    server/chunk_server/chunk_server_lease_service_impl_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_file_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_lease_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/chunk_server_impl.cpp
)

target_link_libraries(chunk_server_lease_service_impl_test
    ${GTEST_BOTH_LIBRARIES}
    leveldb
    protos_shared
    grpc_client_shared
)

add_executable(file_chunk_manager_test
    server/chunk_server/file_chunk_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
)

target_link_libraries(file_chunk_manager_test
    ${GTEST_BOTH_LIBRARIES}
    leveldb
    protos_shared
)

find_package(benchmark REQUIRED)

add_executable(benchmark_app benchmarks/benchmarks.cpp)
target_link_libraries(benchmark_app benchmark::benchmark)

add_executable(benchmark_read benchmarks/read.cpp
    ${PROJECT_SOURCE_DIR}/src/client/client_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client_impl.cpp
)

target_link_libraries(benchmark_read
    benchmark::benchmark
    protos_shared
    common_shared
    grpc_client_shared
    leveldb
)

add_executable(benchmark_write benchmarks/write.cpp
    ${PROJECT_SOURCE_DIR}/src/client/client_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client_impl.cpp
)

target_link_libraries(benchmark_write
    benchmark::benchmark
    protos_shared
    common_shared
    grpc_client_shared
)

add_executable(benchmark_file_chunk_manager benchmarks/chunk_server/file_chunk_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
)

target_link_libraries(benchmark_file_chunk_manager
    benchmark::benchmark
    protos_shared
    common_shared
    grpc_client_shared
    leveldb
)

# stress test
add_executable(stress_write stress_test/write_test.cpp
    ${PROJECT_SOURCE_DIR}/src/client/client_cache_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client.cpp
    ${PROJECT_SOURCE_DIR}/src/client/dfs_client_impl.cpp
)

target_link_libraries(stress_write
    protos_shared
    common_shared
    grpc_client_shared
    pthread
)