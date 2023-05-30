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

static thread_local DfsClientImpl* client_impl_ = nullptr;

google::protobuf::util::Status init_client() {
    if (client_impl_) {
        return AlreadyExistsError("client_impl is already exists");
    }

    client_impl_ = new DfsClientImpl();
    return OkStatus();
}

google::protobuf::util::Status open(const char* filename, unsigned int flag) {
    if (flag | OpenFlag::CREATE) {
        auto status = client_impl_->CreateFile(filename);
        if (!status.ok()) {
            return status;
        }
    }

    return OkStatus();
}

google::protobuf::util::StatusOr<Data> read(const char* filename, size_t offset,
                                            size_t nbytes) {
    auto read_or = client_impl_->ReadFile(filename, offset, nbytes);
    if (!read_or.ok()) {
        return read_or.status();
    }

    auto read_data = read_or.value();

    return Data(read_data.first, read_data.second);
}

google::protobuf::util::StatusOr<size_t> write(const char* filename,
                                               void* buffer, size_t offset,
                                               size_t nbytes) {
    auto write_or = client_impl_->WriteFile(filename, buffer, offset, nbytes);
    return write_or;
}

google::protobuf::util::Status remove(const char* filename) {
    return client_impl_->DeleteFile(filename);
}

google::protobuf::util::Status close(const char* filename) {
    return OkStatus();
}

google::protobuf::util::Status upload(const char* filename) {
    // std::ifstream file(filename);  // 指定要写入的文件名

    // if (file.is_open()) {
    //     std::string line;
    //     std::getline(file, line);
    //     std::cout << line.size() << std::endl;
    //     // filename 为绝对路径
    //     // 获取文件名
    //     auto pos = filename.find_last_of('/');
    //     write(filename.substr(pos), line, 0, line.size());
    //     file.close();
    //     return OkStatus();
    // } else {
    //     std::cout << "无法打开文件" << std::endl;
    //     return UnknownError("can not open file");
    // }
}

google::protobuf::util::Status set(const char* filename, size_t size) {
    absl::Time start_time = absl::Now();
    void* buffer = malloc(size);
    memset(buffer, '0', size);
    absl::Time end_time = absl::Now();
    absl::Duration elapsed_time = end_time - start_time;

    std::cout << "set spend time: " << absl::ToDoubleMilliseconds(elapsed_time)
              << "ms" << std::endl;

    return write(filename, buffer, 0, size).status();
}

void reset_client() {
    delete client_impl_;
    client_impl_ = nullptr;
}

}  // namespace client
}  // namespace dfs