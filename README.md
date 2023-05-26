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
- client_cache_manager，当 client 进行文件操作时，会向 master 请求 metadata，将请求来的 metadata 缓存起来，这样就不需要每次文件操作都向 master 获取 metadata。
- grpc_client，rpc 服务的客户端。
- chunk_cache_manager，chunkserver 用于缓存数据，当 client 写入数据时，先将数据写入到 cache，然后根据 cache 数据与写入数据是否相同，来决定写入到 chunkserver。

## commit message

添加了主服务器对块服务器的心跳包检测，在块服务器宕机时，将其从系统中注销掉。

如果说块服务器上存储的块将该块服务器作为主副本服务器，那么在注销块服务器之后，为数据块分配新的主副本服务器。

这样，元数据中的主副本服务器只有在数据块创建时，或者主副本服务器宕机时进行设置。
