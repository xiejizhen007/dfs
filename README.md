# DFS

## log

- 基于 glog 实现了日志
- 实现了 lock_manager，对文件进行加锁操作，用于之后读写文件。
- metadata_manager，对 file 与 chunk 的 metadata 进行管理，创建删除等。
