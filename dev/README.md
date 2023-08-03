- 用./run.sh编译代码，拷贝到dev/目录的应该有remote_flush_worker/remote_flush_test_server/tcp_server
  
- remote_flush_worker会在localhost:9090端口监听来自tcp_server的连接，接收来自tcp_server的remote_flush_job，本地完成flush任务之后向产生flush任务的remote_flush_test_server发送metadata。这一步暂时不经过tcp_server，也就是memory node。
  
  - 暂时端口都是写死的。为了避免端口冲突，只能一个一个处理。
    
- tcp_server 在localhost:9091 监听来自remote_flush_test_server的flush任务，存储在本地内存，然后在注册的worker中寻找可用的，把flush任务整个发给worker。
  
- remote_flush_test_server是flush任务产生一方，目前因为端口冲突，只能单线程flush。
  
  - 程序生成随机kv对写入不同的column families，自动触发flush或者可以显式调用Flush。
    
  - options:
    
    - 把DBOptions::server_remote_flush设置为true来开启remote_flush_job。
      
    - write_buffer_size也就是memtable的最大大小==64MB；
      
    - max_write_buffer_number==4最多的immutable_memtable数量；
      
    - delayed_write_rate==100<<20设置写入速度限制非常大，模拟写负载很大的场景，专门设置的原因是max_write_buffer_number>=3的时候会默认限制写入速度缓解write stall；
      
    - max_background_compaction==0限制不做compaction
      

```shell
shell1: ./tcp_server
shell2: ./remote_flush_worker
shell3: ./remote_flush_test_worker 1
```