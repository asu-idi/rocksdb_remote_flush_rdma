### Guide

- 用./run.sh编译代码，拷贝到dev/目录的应该有remote_flush_worker/remote_flush_test_server/tcp_server
  

- remote_flush_worker会在本地端口A监听来自tcp_server的连接，接收来自指定tcp_server的remote_flush_job。
  
  - worker本地完成flush任务之后向产生flush任务的remote_flush_test_server发送metadata。这一步暂时不经过tcp_server，也就是memory node。
    
  - 用法是： ./remote_flush_worker (tcp_server ip) (tcp_server port) (local port A)
    
- tcp_server 在localhost:B 监听来自remote_flush_test_server的flush任务，存储在本地内存，然后在注册的worker中寻找可用的，把flush任务整个发给worker。
  
  - 用法是：./tcp_server (local port B)，注册worker用memnode.register_flush_job_executor API。
    
- remote_flush_test_server是flush任务产生一方。
  
  - 程序十个线程生成随机kv对写入不同的column families，自动触发flush或者可以显式调用Flush。
    
  - options:
    
    - 把DBOptions::server_remote_flush设置为true来开启remote_flush_job。
      
    - write_buffer_size也就是memtable的最大大小==64MB；
      
    - max_write_buffer_number==4最多的immutable_memtable数量；
      
    - delayed_write_rate==100<<20设置写入速度限制非常大，模拟写负载很大的场景，专门设置的原因是max_write_buffer_number>=3的时候会默认限制写入速度缓解write stall；
      
    - max_background_compaction==0限制不做compaction
      
  - 用法是：./remote_flush_test_worker 1/0 1开启remote flush
    

- 支持多个写入线程，多个memnode，多个worker。
  

### example

```shell
shell1: ./remote_flush_worker 127.0.0.1 9091 9093

shell2: ./remote_flush_worker 127.0.0.1 9091 9092 # 在tcp_server.cc 注册

shell3: ./remote_flush_test_worker 1 

shell4: ./tcp_server 9091 # 在remote_flush_test_worker.cpp 注册
```

### TODO

- add RDMA support