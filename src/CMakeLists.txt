# # Dependencies

# find_package(protobuf REQUIRED)
# find_package(gRPC CONFIG REQUIRED)

# # Proto files

# set(proto_files
#     ${PROJECT_SOURCE_DIR}/src/protos/hello.proto
# )

# # Add Library target

# add_library(libproto ${proto_files})

# target_link_libraries(libproto
#     PUBLIC
#         protobuf::libprotobuf
#         gRPC::grpc
#         gRPC::grpc++
# )

# target_include_directories(libproto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
# message("libproto current binary dir: ${CMAKE_CURRENT_BINARY_DIR}")

# # Generate protobuf and grpc files in libproto target to cpp

# get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
# protobuf_generate(TARGET libproto LANGUAGE cpp)
# protobuf_generate(TARGET libproto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")

# include_directories(${CMAKE_CURRENT_BINARY_DIR})

# # Client
# aux_source_directory(${PROJECT_SOURCE_DIR}/src/client client_sources)
# add_executable(client ${client_sources})
# target_link_libraries(client
#     PRIVATE
#         libproto
# )

# # Server
# aux_source_directory(${PROJECT_SOURCE_DIR}/src/client server_sources)
# add_executable(server ${server_sources})
# target_link_libraries(server
#     PRIVATE
#         libproto
# )

include_directories(common)
aux_source_directory(${PROJECT_SOURCE_DIR}/src/common common_sources)

add_library(common_shared ${common_sources})

add_subdirectory(client)
add_subdirectory(protos)
add_subdirectory(server)