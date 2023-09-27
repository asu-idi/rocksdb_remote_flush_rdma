``` 
result_$wb_$num_$rdma.txt
```

is the result of

```
./db_bench --db=/tmp/rrtest --num=$num --max_background_jobs=16 --max_background_flushes=5 --max_write_buffer_number=2 --key_size=16 --value_size=1024 --benchmarks=fillrandom --disable_wal=1 --write_buffer_size=$wb*1048576  --threads=2 --num_column_families=4 --memnode_heartbeat_port=10086 --report_fillrandom_latency_and_load=true --track_flush_compaction_stats=true --statistics=true --use_remote_flush=$rdma --memnode_ip=10.145.21.35 --memnode_port=9091 --local_ip=10.145.21.39
```

Setting:

- Server 1: RocksDB instance (Flush job generator)
- Server 2: one idle worker
- Server 3: Memory node