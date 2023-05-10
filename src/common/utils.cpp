#include "src/common/utils.h"

namespace dfs {
namespace common {

google::protobuf::util::Status StatusGrpc2Protobuf(grpc::Status status) {
    const auto msg = status.error_message();
    switch (status.error_code()) {
        case grpc::StatusCode::OK:
            return google::protobuf::util::OkStatus();
        case grpc::StatusCode::NOT_FOUND:
            return google::protobuf::util::NotFoundError(msg);
        case grpc::StatusCode::ALREADY_EXISTS:
            return google::protobuf::util::AlreadyExistsError(msg);
    }

    return google::protobuf::util::InternalError("Unknown error message code");
}

grpc::Status StatusProtobuf2Grpc(google::protobuf::util::Status status) {
    const std::string msg = status.message().ToString();
    if (status.ok()) {
        return grpc::Status(grpc::StatusCode::OK, msg);
    } else if (google::protobuf::util::IsAlreadyExists(status)) {
        return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, msg);
    } else if (google::protobuf::util::IsNotFound(status)) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, msg);
    } else {
        return grpc::Status(grpc::StatusCode::UNKNOWN, msg);
    }
}

}  // namespace common
}  // namespace dfs