#include "src/client/dfs_client.h"

#include <fstream>
#include <iostream>
#include <string>

namespace dfs {
namespace client {

using google::protobuf::util::AlreadyExistsError;
using google::protobuf::util::IsAlreadyExists;
using google::protobuf::util::OkStatus;
using google::protobuf::util::UnknownError;

static DfsClientImpl* client_impl_ = nullptr;

google::protobuf::util::Status init_client() {
    if (client_impl_) {
        return AlreadyExistsError("client_impl is already exists");
    }

    client_impl_ = new DfsClientImpl();
    return OkStatus();
}

google::protobuf::util::Status open(const std::string& filename,
                                    unsigned int flag) {
    if (flag | OpenFlag::CREATE) {
        auto status = client_impl_->CreateFile(filename);
        if (!status.ok()) {
            return status;
        }
    }

    return OkStatus();
}

google::protobuf::util::StatusOr<Data> read(const std::string& filename,
                                            size_t offset, size_t nbytes) {
    auto read_or = client_impl_->ReadFile(filename, offset, nbytes);
    if (!read_or.ok()) {
        return read_or.status();
    }

    auto read_data = read_or.value();

    return Data(read_data.first, read_data.second);
}

google::protobuf::util::StatusOr<size_t> write(const std::string& filename,
                                               const std::string& data,
                                               size_t offset, size_t nbytes) {
    auto write_or = client_impl_->WriteFile(filename, data, offset, nbytes);
    return write_or;
}

google::protobuf::util::Status remove(const std::string& filename) {
    return client_impl_->DeleteFile(filename);
}

google::protobuf::util::Status close(const std::string& filename) {
    return OkStatus();
}

google::protobuf::util::Status upload(const std::string& filename) {
    std::ifstream file(filename);  // 指定要写入的文件名

    if (file.is_open()) {
        std::string line;
        std::getline(file, line);
        std::cout << line.size() << std::endl;
        // filename 为绝对路径
        // 获取文件名
        auto pos = filename.find_last_of('/');
        write(filename.substr(pos), line, 0, line.size());
        file.close();
        return OkStatus();
    } else {
        std::cout << "无法打开文件" << std::endl;
        return UnknownError("can not open file");
    }
}

void reset_client() {
    delete client_impl_;
    client_impl_ = nullptr;
}

}  // namespace client
}  // namespace dfs