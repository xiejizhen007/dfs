cmake_minimum_required(VERSION 3.5)
project(DFS)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${C_FLAG_COMMON} -O3 -g -fPIC -fno-omit-frame-pointer -fno-optimize-sibling-calls")

# 添加宏
add_compile_definitions(CMAKE_SOURCE_DIR="${CMAKE_SOURCE_DIR}")

add_subdirectory(src)
add_subdirectory(test)