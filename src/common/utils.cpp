#include "src/common/utils.h"

namespace dfs {
namespace common {

const std::string calc_md5(const std::string& data) {
    std::string md5(MD5_DIGEST_LENGTH, ' ');
    MD5((const unsigned char*)&data[0], data.size(), (unsigned char*)&md5[0]);
    return md5;
}

const std::string ComputeHash(const std::string& data) {
    std::string hash(EVP_MAX_MD_SIZE, ' ');
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(mdctx, data.data(), data.size());
    unsigned int len;
    EVP_DigestFinal_ex(mdctx, reinterpret_cast<unsigned char*>(&hash[0]), &len);
    hash.resize(len);
    EVP_MD_CTX_free(mdctx);
    return hash;
}

google::protobuf::util::Status StatusGrpc2Protobuf(grpc::Status status) {
    const auto msg = status.error_message();
    switch (status.error_code()) {
        case grpc::StatusCode::OK:
            return google::protobuf::util::OkStatus();
        case grpc::StatusCode::NOT_FOUND:
            return google::protobuf::util::NotFoundError(msg);
        case grpc::StatusCode::ALREADY_EXISTS:
            return google::protobuf::util::AlreadyExistsError(msg);
        case grpc::StatusCode::UNKNOWN:
            return google::protobuf::util::UnknownError(msg);
    }

    return google::protobuf::util::InternalError("Unknown error message code" + msg);
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