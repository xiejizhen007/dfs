# DFS

## log

- 基于 glog 实现了日志
- 实现了 lock_manager，对文件进行加锁操作，用于之后读写文件。
- metadata_manager，对 file 与 chunk 的 metadata 进行管理，创建删除等。
- master_metadata_service_impl，是对 rpc 的实现，用于与 client，chunk server 进行通信，对 metadata 进行简单的基本操作。
- file_chunk_manager，利用 leveldb 实现持久性存储，实现对 chunk 的 create，read，write，delete 等，以及对 chunk 简单的版本控制。
- chunk_server_file_service_impl，响应客户端对文件的请求。
- chunk_server_manager 对 chunkserver 的管理，将 chunkserver 的信息保存在 master 的内存中，以便快速读取。
- chunk_server_manager_service_impl，具体的 rpc 实现，响应 chunkserver 给 master 发送的数据（chunk metadata）。
