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

## HDFS

###### 配置一台memnode，两台compNode，1generator+1worker。HDFS跑在memnode上面。

- 在每台机器/etc/hosts中添加memnode generator worker ，类似这样：

```shell
vim /etc/hosts

10.206.16.16 memnode

10.206.16.17 compnode-generator

10.206.16.8 compnode-worker
```

- memnode配置 ${HADOOP_HOME}/etc/hadoop/core-site.xml：

```xml
<configuration>

<property>

<name>fs.defaultFS</name>

<value>hdfs://memnode:9000</value>

</property>

</configuration>
```

- ${HADOOP_HOME}/sbin/start-dfs.sh 启动不了hdfs，应该需要：
  
- 脚本添加声明。stop-dfs.sh同理
  

```shell
HDFS_DATANODE_USER=root

HADOOP_SECURE_DN_USER=hdfs

HDFS_NAMENODE_USER=root

HDFS_SECONDARYNAMENODE_USER=root
```

- chsh -s $(which bash) 默认终端需要是bash
  
- ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh 添加
  

```shell
JAVA_HOME=....
```

- compNode 不修改hdfs-site.xml，${HADOOP_HOME}/etc/hadoop/core-site.xml配置为：

```xml
<configuration>

<property>

<name>fs.defaultFS</name>

<value>hdfs://memnode:9000</value>

</property>

</configuration>
```

### db_bench

```shell
./db_bench --db=/tmp/rrtest --num=1000000 --key_size=50 --value_size=100 --benchmarks=readwhilewriting --disable_wal=1 --max_bytes_for_level_base=4194304 --num_levels=1 --target_file_size_base=4194304 --write_buffer_size=4194304 --level0_file_num_compaction_trigger=10 --level0_slowdown_writes_trigger=10 --level0_stop_writes_trigger=10 --use_remote_flush=1 --memnode_ip=127.0.0.1 --memnode_port=9091 --local_ip=127.0.0.1

--use_remote_flush=1 # 开启remote flush

--memnode_ip=127.0.0.1

--memnode_port=9091

--local_ip=127.0.0.1 # 本机内网ip
```

- 在 env_posix.cc 修改运行HDFS的主机ip&port。

####

#### scheduler 相关

- scheduler 计算每个节点的负载，计算公式为：

$$
val=\frac{current\ HDFS\ output}{max\_hdfs\_io}+\frac{current\ Background\ Thread}{max\_background\_job\_num}
$$

选取最小值对应的节点分配flush job。

- 用法： from dev/remote_flush_test_server.cpp
  

```cpp
  if (opt.server_remote_flush) {
    db->register_memnode("127.0.0.1", 9091);
    auto* pd_client = new PDClient{10089};
    pd_client->match_memnode_for_request();
    db->register_pd_client(pd_client);
    printf("pd_client registered\n");
  }
```

非memnode需要创建一个PDClient{port} ，在本机的port端口等待memnode发起连接。然后rocksdb instance调用match_memnode_for_request()；worker调用match_memnode_for_heartbeat()建立连接。最后调用DBImpl::register_pd_client(client)注册到db实例上。

#### TODO

1. 在db_bench中，**需要设置**：
  

```shell
--heartbeat_local_port=10089 
```

来指定一个端口。

2. 照着tcp_server.cc在RDMANode上添加PlacementDriver pd_;，rdma_server运行的时候调用：
  

```cpp
pd_add_generator(ip,port);
pd_add_worker(ip,port);
```

，port是用来传心跳包的。

3. RemoteFlushJob里的RDMA相关的变量，被我用宏包起来了，麻烦确认在ROCKSDB_RDMA enable的时候是不是正确的。
  
  **为了检验正确性，按照dev/remote_flush_test_server.cpp稍微改改，确保写入->flush->读取，能够读到正确的值。**