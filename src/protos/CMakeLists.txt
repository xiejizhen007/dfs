# Dependencies

find_package(protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Proto files

set(proto_files
    ${PROJECT_SOURCE_DIR}/src/protos/hello.proto
)

# Add Library target

add_library(libproto ${proto_files})

target_link_libraries(libproto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++
)

target_include_directories(libproto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# Generate protobuf and grpc files in libproto target to cpp

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET libproto LANGUAGE cpp)
protobuf_generate(TARGET libproto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")