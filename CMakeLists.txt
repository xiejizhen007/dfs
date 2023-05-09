cmake_minimum_required(VERSION 3.5)
project(DFS)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 17)

add_subdirectory(src)
add_subdirectory(test)