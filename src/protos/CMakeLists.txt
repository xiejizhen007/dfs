# Dependencies

find_package(protobuf REQUIRED)
message(STATUS "${protobuf_VERSION}")
find_package(gRPC CONFIG REQUIRED)
message(STATUS "${gRPC_VERSION}")

# Proto files

set(proto_files
    ${PROJECT_SOURCE_DIR}/src/protos/hello.proto
    ${PROJECT_SOURCE_DIR}/src/protos/chunk_server.proto
    ${PROJECT_SOURCE_DIR}/src/protos/metadata.proto
    # hello.proto
    # chunk_server.proto
    # metadata.proto
)

# Add Library target

add_library(protos_shared ${proto_files})
message("${proto_files}")

target_link_libraries(protos_shared
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++
)

target_include_directories(protos_shared PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# Generate protobuf and grpc files in libproto target to cpp

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET protos_shared LANGUAGE cpp)
protobuf_generate(TARGET protos_shared LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")