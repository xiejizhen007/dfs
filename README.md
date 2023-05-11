# DFS

## log

- 基于 glog 实现了日志
- 实现了 lock_manager，对文件进行加锁操作，用于之后读写文件。
- metadata_manager，对 file 与 chunk 的 metadata 进行管理，创建删除等。
- master_metadata_service_impl，是对 rpc 的实现，用于与 client，chunk server 进行通信，对 metadata 进行简单的基本操作。
- file_chunk_manager，利用 leveldb 实现持久性存储，实现对 chunk 的 create，read，write，delete 等。
