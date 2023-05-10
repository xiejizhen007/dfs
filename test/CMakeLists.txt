# Add the Google Test library
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# find_package(absl REQUIRED)
# include_directories(${ABSL_INCLUDE_DIRS})
# message("absl version: ${absl_VERSION}")

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