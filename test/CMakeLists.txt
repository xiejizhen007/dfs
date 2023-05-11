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

add_executable(lock_manager_test
    server/master_server/lock_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/master_server/lock_manager.cpp
)

target_link_libraries(lock_manager_test
    ${GTEST_BOTH_LIBRARIES}
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

add_executable(file_chunk_manager_test
    server/chunk_server/file_chunk_manager_test.cpp
    ${PROJECT_SOURCE_DIR}/src/server/chunk_server/file_chunk_manager.cpp
)

target_link_libraries(file_chunk_manager_test
    ${GTEST_BOTH_LIBRARIES}
    leveldb
    protos_shared
)