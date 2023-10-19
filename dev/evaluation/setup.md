```shell
# hdfs 
# hdfs dfsadmin -report
# experiment setup (use pseudo distributed guide)
☁  rocksdb_remote_flush [dev] hdfs dfsadmin -report                             
Configured Capacity: 960098545664 (894.16 GB)
Present Capacity: 909930714948 (847.44 GB)
DFS Remaining: 892913908931 (831.59 GB)
DFS Used: 17016806017 (15.85 GB)
DFS Used%: 1.87%
Replicated Blocks:
        Under replicated blocks: 241
        Blocks with corrupt replicas: 0
        Missing blocks: 0
        Missing blocks (with replication factor 1): 0
        Low redundancy blocks with highest priority to recover: 241
        Pending deletion blocks: 0
Erasure Coded Block Groups: 
        Low redundancy block groups: 0
        Block groups with corrupt internal blocks: 0
        Missing block groups: 0
        Low redundancy blocks with highest priority to recover: 0
        Pending deletion blocks: 0

-------------------------------------------------
Live datanodes (1):

Name: 10.10.1.3:9866 (hdfs-storage-node)
Hostname: apt035.apt.emulab.net
Decommission Status : Normal
Configured Capacity: 960098545664 (894.16 GB)
DFS Used: 17016806017 (15.85 GB)
Non DFS Used: 738137471 (703.94 MB)
DFS Remaining: 892913908931 (831.59 GB)
DFS Used%: 1.77%
DFS Remaining%: 93.00%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 37
Last contact: Wed Oct 18 18:49:45 MDT 2023
Last Block Report: Wed Oct 18 16:11:28 MDT 2023
Num of Blocks: 241
```

```shell
# 4 thread local flush
./db_bench --db=/tmp/rrtest --num=3000000 --max_background_jobs=4 --max_write_buffer_number=4 \
--min_write_buffer_number_to_merge=2 --benchmarks=fillrandom --key_size=16 --value_size=1024 \
--disable_wal=1 --write_buffer_size=$(expr 64 \* 1048576) --threads=$(nproc) --num_column_families=8 \
--memnode_heartbeat_port=10086 --report_fillrandom_latency_and_load=true --track_flush_compaction_stats=true \
--statistics=true --use_remote_flush=0 --memnode_ip=10.10.1.5 --memnode_port=9091 --local_ip=10.10.1.1 \
--level0_slowdown_writes_trigger=100000 --level0_stop_writes_trigger=1000000 &> log.log


fillrandom   :     902.046 micros/op 35463 ops/sec 2707.039 seconds 96000000 operations;   35.2 MB/s
140497918867200:::Construct traditional flush job 
140497918867200:::Construct traditional flush job 
140497918867200:::Construct traditional flush job 
140497918867200:::Construct traditional flush job 
STATISTICS:
rocksdb.block.cache.miss COUNT : 14077912
rocksdb.block.cache.hit COUNT : 0
rocksdb.block.cache.add COUNT : 0
rocksdb.block.cache.add.failures COUNT : 0
rocksdb.block.cache.index.miss COUNT : 0
rocksdb.block.cache.index.hit COUNT : 0
rocksdb.block.cache.index.add COUNT : 0
rocksdb.block.cache.index.bytes.insert COUNT : 0
rocksdb.block.cache.filter.miss COUNT : 0
rocksdb.block.cache.filter.hit COUNT : 0
rocksdb.block.cache.filter.add COUNT : 0
rocksdb.block.cache.filter.bytes.insert COUNT : 0
rocksdb.block.cache.data.miss COUNT : 14077913
rocksdb.block.cache.data.hit COUNT : 0
rocksdb.block.cache.data.add COUNT : 0
rocksdb.block.cache.data.bytes.insert COUNT : 0
rocksdb.block.cache.bytes.read COUNT : 0
rocksdb.block.cache.bytes.write COUNT : 0
rocksdb.bloom.filter.useful COUNT : 0
rocksdb.bloom.filter.full.positive COUNT : 0
rocksdb.bloom.filter.full.true.positive COUNT : 0
rocksdb.persistent.cache.hit COUNT : 0
rocksdb.persistent.cache.miss COUNT : 0
rocksdb.sim.block.cache.hit COUNT : 0
rocksdb.sim.block.cache.miss COUNT : 0
rocksdb.memtable.hit COUNT : 0
rocksdb.memtable.miss COUNT : 0
rocksdb.l0.hit COUNT : 0
rocksdb.l1.hit COUNT : 0
rocksdb.l2andup.hit COUNT : 0
rocksdb.compaction.key.drop.new COUNT : 46231071
rocksdb.compaction.key.drop.obsolete COUNT : 0
rocksdb.compaction.key.drop.range_del COUNT : 0
rocksdb.compaction.key.drop.user COUNT : 0
rocksdb.compaction.range_del.drop.obsolete COUNT : 0
rocksdb.compaction.optimized.del.drop.obsolete COUNT : 0
rocksdb.compaction.cancelled COUNT : 0
rocksdb.number.keys.written COUNT : 96000000
rocksdb.number.keys.read COUNT : 0
rocksdb.number.keys.updated COUNT : 0
rocksdb.bytes.written COUNT : 100516247709
rocksdb.bytes.read COUNT : 0
rocksdb.number.db.seek COUNT : 0
rocksdb.number.db.next COUNT : 0
rocksdb.number.db.prev COUNT : 0
rocksdb.number.db.seek.found COUNT : 0
rocksdb.number.db.next.found COUNT : 0
rocksdb.number.db.prev.found COUNT : 0
rocksdb.db.iter.bytes.read COUNT : 0
rocksdb.no.file.opens COUNT : 739
rocksdb.no.file.errors COUNT : 0
rocksdb.stall.micros COUNT : 1645802395
rocksdb.db.mutex.wait.micros COUNT : 0
rocksdb.number.multiget.get COUNT : 0
rocksdb.number.multiget.keys.read COUNT : 0
rocksdb.number.multiget.bytes.read COUNT : 0
rocksdb.number.merge.failures COUNT : 0
rocksdb.bloom.filter.prefix.checked COUNT : 0
rocksdb.bloom.filter.prefix.useful COUNT : 0
rocksdb.number.reseeks.iteration COUNT : 0
rocksdb.getupdatessince.calls COUNT : 0
rocksdb.wal.synced COUNT : 0
rocksdb.wal.bytes COUNT : 0
rocksdb.write.self COUNT : 0
rocksdb.write.other COUNT : 0
rocksdb.write.wal COUNT : 0
rocksdb.compact.read.bytes COUNT : 34296643177
rocksdb.compact.write.bytes COUNT : 6102018427
rocksdb.flush.write.bytes COUNT : 47932332335
rocksdb.compact.read.marked.bytes COUNT : 0
rocksdb.compact.read.periodic.bytes COUNT : 0
rocksdb.compact.read.ttl.bytes COUNT : 0
rocksdb.compact.write.marked.bytes COUNT : 0
rocksdb.compact.write.periodic.bytes COUNT : 0
rocksdb.compact.write.ttl.bytes COUNT : 0
rocksdb.number.direct.load.table.properties COUNT : 0
rocksdb.number.superversion_acquires COUNT : 0
rocksdb.number.superversion_releases COUNT : 0
rocksdb.number.superversion_cleanups COUNT : 0
rocksdb.number.block.compressed COUNT : 22069205
rocksdb.number.block.decompressed COUNT : 14078651
rocksdb.number.block.not_compressed COUNT : 0
rocksdb.merge.operation.time.nanos COUNT : 0
rocksdb.filter.operation.time.nanos COUNT : 0
rocksdb.row.cache.hit COUNT : 0
rocksdb.row.cache.miss COUNT : 0
rocksdb.read.amp.estimate.useful.bytes COUNT : 0
rocksdb.read.amp.total.read.bytes COUNT : 0
rocksdb.number.rate_limiter.drains COUNT : 0
rocksdb.number.iter.skip COUNT : 0
rocksdb.blobdb.num.put COUNT : 0
rocksdb.blobdb.num.write COUNT : 0
rocksdb.blobdb.num.get COUNT : 0
rocksdb.blobdb.num.multiget COUNT : 0
rocksdb.blobdb.num.seek COUNT : 0
rocksdb.blobdb.num.next COUNT : 0
rocksdb.blobdb.num.prev COUNT : 0
rocksdb.blobdb.num.keys.written COUNT : 0
rocksdb.blobdb.num.keys.read COUNT : 0
rocksdb.blobdb.bytes.written COUNT : 0
rocksdb.blobdb.bytes.read COUNT : 0
rocksdb.blobdb.write.inlined COUNT : 0
rocksdb.blobdb.write.inlined.ttl COUNT : 0
rocksdb.blobdb.write.blob COUNT : 0
rocksdb.blobdb.write.blob.ttl COUNT : 0
rocksdb.blobdb.blob.file.bytes.written COUNT : 0
rocksdb.blobdb.blob.file.bytes.read COUNT : 0
rocksdb.blobdb.blob.file.synced COUNT : 0
rocksdb.blobdb.blob.index.expired.count COUNT : 0
rocksdb.blobdb.blob.index.expired.size COUNT : 0
rocksdb.blobdb.blob.index.evicted.count COUNT : 0
rocksdb.blobdb.blob.index.evicted.size COUNT : 0
rocksdb.blobdb.gc.num.files COUNT : 0
rocksdb.blobdb.gc.num.new.files COUNT : 0
rocksdb.blobdb.gc.failures COUNT : 0
rocksdb.blobdb.gc.num.keys.relocated COUNT : 0
rocksdb.blobdb.gc.bytes.relocated COUNT : 0
rocksdb.blobdb.fifo.num.files.evicted COUNT : 0
rocksdb.blobdb.fifo.num.keys.evicted COUNT : 0
rocksdb.blobdb.fifo.bytes.evicted COUNT : 0
rocksdb.txn.overhead.mutex.prepare COUNT : 0
rocksdb.txn.overhead.mutex.old.commit.map COUNT : 0
rocksdb.txn.overhead.duplicate.key COUNT : 0
rocksdb.txn.overhead.mutex.snapshot COUNT : 0
rocksdb.txn.get.tryagain COUNT : 0
rocksdb.number.multiget.keys.found COUNT : 0
rocksdb.num.iterator.created COUNT : 0
rocksdb.num.iterator.deleted COUNT : 0
rocksdb.block.cache.compression.dict.miss COUNT : 0
rocksdb.block.cache.compression.dict.hit COUNT : 0
rocksdb.block.cache.compression.dict.add COUNT : 0
rocksdb.block.cache.compression.dict.bytes.insert COUNT : 0
rocksdb.block.cache.add.redundant COUNT : 0
rocksdb.block.cache.index.add.redundant COUNT : 0
rocksdb.block.cache.filter.add.redundant COUNT : 0
rocksdb.block.cache.data.add.redundant COUNT : 0
rocksdb.block.cache.compression.dict.add.redundant COUNT : 0
rocksdb.files.marked.trash COUNT : 0
rocksdb.files.deleted.immediately COUNT : 432
rocksdb.error.handler.bg.errro.count COUNT : 0
rocksdb.error.handler.bg.io.errro.count COUNT : 0
rocksdb.error.handler.bg.retryable.io.errro.count COUNT : 0
rocksdb.error.handler.autoresume.count COUNT : 0
rocksdb.error.handler.autoresume.retry.total.count COUNT : 0
rocksdb.error.handler.autoresume.success.count COUNT : 0
rocksdb.memtable.payload.bytes.at.flush COUNT : 100151206144
rocksdb.memtable.garbage.bytes.at.flush COUNT : 18163246416
rocksdb.secondary.cache.hits COUNT : 0
rocksdb.verify_checksum.read.bytes COUNT : 0
rocksdb.backup.read.bytes COUNT : 0
rocksdb.backup.write.bytes COUNT : 0
rocksdb.remote.compact.read.bytes COUNT : 0
rocksdb.remote.compact.write.bytes COUNT : 0
rocksdb.hot.file.read.bytes COUNT : 0
rocksdb.warm.file.read.bytes COUNT : 0
rocksdb.cold.file.read.bytes COUNT : 0
rocksdb.hot.file.read.count COUNT : 0
rocksdb.warm.file.read.count COUNT : 0
rocksdb.cold.file.read.count COUNT : 0
rocksdb.last.level.read.bytes COUNT : 0
rocksdb.last.level.read.count COUNT : 0
rocksdb.non.last.level.read.bytes COUNT : 34660884146
rocksdb.non.last.level.read.count COUNT : 14078886
rocksdb.block.checksum.compute.count COUNT : 14079390
rocksdb.multiget.coroutine.count COUNT : 0
rocksdb.blobdb.cache.miss COUNT : 0
rocksdb.blobdb.cache.hit COUNT : 0
rocksdb.blobdb.cache.add COUNT : 0
rocksdb.blobdb.cache.add.failures COUNT : 0
rocksdb.blobdb.cache.bytes.read COUNT : 0
rocksdb.blobdb.cache.bytes.write COUNT : 0
rocksdb.read.async.micros COUNT : 0
rocksdb.async.read.error.count COUNT : 0
rocksdb.secondary.cache.filter.hits COUNT : 0
rocksdb.secondary.cache.index.hits COUNT : 0
rocksdb.secondary.cache.data.hits COUNT : 0
rocksdb.table.open.prefetch.tail.miss COUNT : 235
rocksdb.table.open.prefetch.tail.hit COUNT : 2721
rocksdb.db.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.micros P50 : 350.605972 P95 : 4855.499733 P99 : 7032.584876 P100 : 296205.000000 COUNT : 95999999 SUM : 83917103559
rocksdb.compaction.times.micros P50 : 227500000.000000 P95 : 550500000.000000 P99 : 606852656.000000 P100 : 606852656.000000 COUNT : 27 SUM : 7167532925
rocksdb.compaction.times.cpu_micros P50 : 135000000.000000 P95 : 337600000.000000 P99 : 356579434.000000 P100 : 356579434.000000 COUNT : 27 SUM : 4198557844
rocksdb.subcompaction.setup.times.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.sync.micros P50 : 584082.733813 P95 : 1031159.090909 P99 : 1454100.000000 P100 : 1572643.000000 COUNT : 637 SUM : 412844208
rocksdb.compaction.outfile.sync.micros P50 : 250000.000000 P95 : 1013000.000000 P99 : 1319703.000000 P100 : 1319703.000000 COUNT : 104 SUM : 36344747
rocksdb.wal.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.manifest.file.sync.micros P50 : 41688.888889 P95 : 62440.476190 P99 : 145400.000000 P100 : 487212.000000 COUNT : 682 SUM : 29554630
rocksdb.table.open.io.micros P50 : 4036.811024 P95 : 9103.875000 P99 : 10400.200000 P100 : 112853.000000 COUNT : 739 SUM : 3398425
rocksdb.db.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.block.compaction.micros P50 : 445.936852 P95 : 573.880769 P99 : 777.821297 P100 : 734291.000000 COUNT : 14077918 SUM : 6177362859
rocksdb.read.block.get.micros P50 : 1551.219512 P95 : 5690.919540 P99 : 6438.413793 P100 : 6748.000000 COUNT : 739 SUM : 1520205
rocksdb.write.raw.block.micros P50 : 13.790228 P95 : 28.708243 P99 : 33.770262 P100 : 527744.000000 COUNT : 22070687 SUM : 494866968
rocksdb.numfiles.in.singlecompaction P50 : 15.000000 P95 : 43.000000 P99 : 43.000000 P100 : 43.000000 COUNT : 30 SUM : 552
rocksdb.db.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.stall P50 : 2310.768806 P95 : 4206.702448 P99 : 4714.260616 P100 : 293466.000000 COUNT : 894726 SUM : 1646655759
rocksdb.sst.read.micros P50 : 363.632852 P95 : 561.871862 P99 : 640.305847 P100 : 734123.000000 COUNT : 14078895 SUM : 5582450764
rocksdb.num.subcompactions.scheduled P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.read P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.write P50 : 4066.882492 P95 : 13960.480631 P99 : 21622.093063 P100 : 33452.000000 COUNT : 17354220 SUM : 100516247709
rocksdb.bytes.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.compressed P50 : 3650.010366 P95 : 4325.046367 P99 : 4385.049567 P100 : 742932.000000 COUNT : 22069206 SUM : 93019620731
rocksdb.bytes.decompressed P50 : 3650.025413 P95 : 4325.073409 P99 : 4385.077675 P100 : 742932.000000 COUNT : 14078663 SUM : 59496586503
rocksdb.compression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.decompression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.num.merge_operands P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.key.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.value.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.next.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.prev.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.read.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.compression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.decompression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.flush.micros P50 : 4280495.356037 P95 : 6283162.939297 P99 : 6462255.591054 P100 : 7062356.000000 COUNT : 637 SUM : 2666952481
rocksdb.sst.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.index.and.filter.blocks.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.sst.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.error.handler.autoresume.retry.count P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.read.bytes P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.poll.wait.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.prefetched.bytes.discarded P50 : 98.063830 P95 : 1031.000000 P99 : 1031.000000 P100 : 1031.000000 COUNT : 1173 SUM : 539191
rocksdb.multiget.io.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.level.read.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.prefetch.abort.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.open.prefetch.tail.read.bytes P50 : 352256.000000 P95 : 376935.326843 P99 : 499795.000000 P100 : 528384.000000 COUNT : 739 SUM : 266125312

140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
140498578447360:::StartToTransferRemoteMemTable 
140497918867200:::Construct traditional flush job 
Destroying HdfsFileSystem(hdfs://hdfs-storage-node:9000/)
```


```shell
# sufficient thread local flush
☁  dev [dev] ./db_bench --db=/tmp/rrtest --num=3000000 --max_background_jobs=70 --max_write_buffer_number=4 \
--min_write_buffer_number_to_merge=2 --benchmarks=fillrandom --key_size=16 --value_size=1024 \
--disable_wal=1 --write_buffer_size=$(expr 64 \* 1048576) --threads=$(nproc) --num_column_families=8 \
--memnode_heartbeat_port=10086 --report_fillrandom_latency_and_load=true --track_flush_compaction_stats=true \
--statistics=true --use_remote_flush=0 --memnode_ip=10.10.1.5 --memnode_port=9091 --local_ip=10.10.1.1 \
--level0_slowdown_writes_trigger=100000 --level0_stop_writes_trigger=1000000 &> log.log

fillrandom   :     565.164 micros/op 56579 ops/sec 1696.715 seconds 96000000 operations;   56.1 MB/s
STATISTICS:
rocksdb.block.cache.miss COUNT : 19725270
rocksdb.block.cache.hit COUNT : 0
rocksdb.block.cache.add COUNT : 0
rocksdb.block.cache.add.failures COUNT : 0
rocksdb.block.cache.index.miss COUNT : 0
rocksdb.block.cache.index.hit COUNT : 0
rocksdb.block.cache.index.add COUNT : 0
rocksdb.block.cache.index.bytes.insert COUNT : 0
rocksdb.block.cache.filter.miss COUNT : 0
rocksdb.block.cache.filter.hit COUNT : 0
rocksdb.block.cache.filter.add COUNT : 0
rocksdb.block.cache.filter.bytes.insert COUNT : 0
rocksdb.block.cache.data.miss COUNT : 19725272
rocksdb.block.cache.data.hit COUNT : 0
rocksdb.block.cache.data.add COUNT : 0
rocksdb.block.cache.data.bytes.insert COUNT : 0
rocksdb.block.cache.bytes.read COUNT : 0
rocksdb.block.cache.bytes.write COUNT : 0
rocksdb.bloom.filter.useful COUNT : 0
rocksdb.bloom.filter.full.positive COUNT : 0
rocksdb.bloom.filter.full.true.positive COUNT : 0
rocksdb.persistent.cache.hit COUNT : 0
rocksdb.persistent.cache.miss COUNT : 0
rocksdb.sim.block.cache.hit COUNT : 0
rocksdb.sim.block.cache.miss COUNT : 0
rocksdb.memtable.hit COUNT : 0
rocksdb.memtable.miss COUNT : 0
rocksdb.l0.hit COUNT : 0
rocksdb.l1.hit COUNT : 0
rocksdb.l2andup.hit COUNT : 0
rocksdb.compaction.key.drop.new COUNT : 27928300
rocksdb.compaction.key.drop.obsolete COUNT : 0
rocksdb.compaction.key.drop.range_del COUNT : 0
rocksdb.compaction.key.drop.user COUNT : 0
rocksdb.compaction.range_del.drop.obsolete COUNT : 0
rocksdb.compaction.optimized.del.drop.obsolete COUNT : 0
rocksdb.compaction.cancelled COUNT : 0
rocksdb.number.keys.written COUNT : 96000000
rocksdb.number.keys.read COUNT : 0
rocksdb.number.keys.updated COUNT : 0
rocksdb.bytes.written COUNT : 100573299175
rocksdb.bytes.read COUNT : 0
rocksdb.number.db.seek COUNT : 0
rocksdb.number.db.next COUNT : 0
rocksdb.number.db.prev COUNT : 0
rocksdb.number.db.seek.found COUNT : 0
rocksdb.number.db.next.found COUNT : 0
rocksdb.number.db.prev.found COUNT : 0
rocksdb.db.iter.bytes.read COUNT : 0
rocksdb.no.file.opens COUNT : 944
rocksdb.no.file.errors COUNT : 0
rocksdb.stall.micros COUNT : 256686920
rocksdb.db.mutex.wait.micros COUNT : 0
rocksdb.number.multiget.get COUNT : 0
rocksdb.number.multiget.keys.read COUNT : 0
rocksdb.number.multiget.bytes.read COUNT : 0
rocksdb.number.merge.failures COUNT : 0
rocksdb.bloom.filter.prefix.checked COUNT : 0
rocksdb.bloom.filter.prefix.useful COUNT : 0
rocksdb.number.reseeks.iteration COUNT : 0
rocksdb.getupdatessince.calls COUNT : 0
rocksdb.wal.synced COUNT : 0
rocksdb.wal.bytes COUNT : 0
rocksdb.write.self COUNT : 0
rocksdb.write.other COUNT : 0
rocksdb.write.wal COUNT : 0
rocksdb.compact.read.bytes COUNT : 48299448488
rocksdb.compact.write.bytes COUNT : 31198641206
rocksdb.flush.write.bytes COUNT : 49494962032
rocksdb.compact.read.marked.bytes COUNT : 0
rocksdb.compact.read.periodic.bytes COUNT : 0
rocksdb.compact.read.ttl.bytes COUNT : 0
rocksdb.compact.write.marked.bytes COUNT : 0
rocksdb.compact.write.periodic.bytes COUNT : 0
rocksdb.compact.write.ttl.bytes COUNT : 0
rocksdb.number.direct.load.table.properties COUNT : 0
rocksdb.number.superversion_acquires COUNT : 0
rocksdb.number.superversion_releases COUNT : 0
rocksdb.number.superversion_cleanups COUNT : 0
rocksdb.number.block.compressed COUNT : 32938018
rocksdb.number.block.decompressed COUNT : 19726210
rocksdb.number.block.not_compressed COUNT : 0
rocksdb.merge.operation.time.nanos COUNT : 0
rocksdb.filter.operation.time.nanos COUNT : 0
rocksdb.row.cache.hit COUNT : 0
rocksdb.row.cache.miss COUNT : 0
rocksdb.read.amp.estimate.useful.bytes COUNT : 0
rocksdb.read.amp.total.read.bytes COUNT : 0
rocksdb.number.rate_limiter.drains COUNT : 0
rocksdb.number.iter.skip COUNT : 0
rocksdb.blobdb.num.put COUNT : 0
rocksdb.blobdb.num.write COUNT : 0
rocksdb.blobdb.num.get COUNT : 0
rocksdb.blobdb.num.multiget COUNT : 0
rocksdb.blobdb.num.seek COUNT : 0
rocksdb.blobdb.num.next COUNT : 0
rocksdb.blobdb.num.prev COUNT : 0
rocksdb.blobdb.num.keys.written COUNT : 0
rocksdb.blobdb.num.keys.read COUNT : 0
rocksdb.blobdb.bytes.written COUNT : 0
rocksdb.blobdb.bytes.read COUNT : 0
rocksdb.blobdb.write.inlined COUNT : 0
rocksdb.blobdb.write.inlined.ttl COUNT : 0
rocksdb.blobdb.write.blob COUNT : 0
rocksdb.blobdb.write.blob.ttl COUNT : 0
rocksdb.blobdb.blob.file.bytes.written COUNT : 0
rocksdb.blobdb.blob.file.bytes.read COUNT : 0
rocksdb.blobdb.blob.file.synced COUNT : 0
rocksdb.blobdb.blob.index.expired.count COUNT : 0
rocksdb.blobdb.blob.index.expired.size COUNT : 0
rocksdb.blobdb.blob.index.evicted.count COUNT : 0
rocksdb.blobdb.blob.index.evicted.size COUNT : 0
rocksdb.blobdb.gc.num.files COUNT : 0
rocksdb.blobdb.gc.num.new.files COUNT : 0
rocksdb.blobdb.gc.failures COUNT : 0
rocksdb.blobdb.gc.num.keys.relocated COUNT : 0
rocksdb.blobdb.gc.bytes.relocated COUNT : 0
rocksdb.blobdb.fifo.num.files.evicted COUNT : 0
rocksdb.blobdb.fifo.num.keys.evicted COUNT : 0
rocksdb.blobdb.fifo.bytes.evicted COUNT : 0
rocksdb.txn.overhead.mutex.prepare COUNT : 0
rocksdb.txn.overhead.mutex.old.commit.map COUNT : 0
rocksdb.txn.overhead.duplicate.key COUNT : 0
rocksdb.txn.overhead.mutex.snapshot COUNT : 0
rocksdb.txn.get.tryagain COUNT : 0
rocksdb.number.multiget.keys.found COUNT : 0
rocksdb.num.iterator.created COUNT : 0
rocksdb.num.iterator.deleted COUNT : 0
rocksdb.block.cache.compression.dict.miss COUNT : 0
rocksdb.block.cache.compression.dict.hit COUNT : 0
rocksdb.block.cache.compression.dict.add COUNT : 0
rocksdb.block.cache.compression.dict.bytes.insert COUNT : 0
rocksdb.block.cache.add.redundant COUNT : 0
rocksdb.block.cache.index.add.redundant COUNT : 0
rocksdb.block.cache.filter.add.redundant COUNT : 0
rocksdb.block.cache.data.add.redundant COUNT : 0
rocksdb.block.cache.compression.dict.add.redundant COUNT : 0
rocksdb.files.marked.trash COUNT : 0
rocksdb.files.deleted.immediately COUNT : 673
rocksdb.error.handler.bg.errro.count COUNT : 0
rocksdb.error.handler.bg.io.errro.count COUNT : 0
rocksdb.error.handler.bg.retryable.io.errro.count COUNT : 0
rocksdb.error.handler.autoresume.count COUNT : 0
rocksdb.error.handler.autoresume.retry.total.count COUNT : 0
rocksdb.error.handler.autoresume.success.count COUNT : 0
rocksdb.memtable.payload.bytes.at.flush COUNT : 99752979768
rocksdb.memtable.garbage.bytes.at.flush COUNT : 15097545640
rocksdb.secondary.cache.hits COUNT : 0
rocksdb.verify_checksum.read.bytes COUNT : 0
rocksdb.backup.read.bytes COUNT : 0
rocksdb.backup.write.bytes COUNT : 0
rocksdb.remote.compact.read.bytes COUNT : 0
rocksdb.remote.compact.write.bytes COUNT : 0
rocksdb.hot.file.read.bytes COUNT : 0
rocksdb.warm.file.read.bytes COUNT : 0
rocksdb.cold.file.read.bytes COUNT : 0
rocksdb.hot.file.read.count COUNT : 0
rocksdb.warm.file.read.count COUNT : 0
rocksdb.cold.file.read.count COUNT : 0
rocksdb.last.level.read.bytes COUNT : 0
rocksdb.last.level.read.count COUNT : 0
rocksdb.non.last.level.read.bytes COUNT : 48569091920
rocksdb.non.last.level.read.count COUNT : 19726384
rocksdb.block.checksum.compute.count COUNT : 19727157
rocksdb.multiget.coroutine.count COUNT : 0
rocksdb.blobdb.cache.miss COUNT : 0
rocksdb.blobdb.cache.hit COUNT : 0
rocksdb.blobdb.cache.add COUNT : 0
rocksdb.blobdb.cache.add.failures COUNT : 0
rocksdb.blobdb.cache.bytes.read COUNT : 0
rocksdb.blobdb.cache.bytes.write COUNT : 0
rocksdb.read.async.micros COUNT : 0
rocksdb.async.read.error.count COUNT : 0
rocksdb.secondary.cache.filter.hits COUNT : 0
rocksdb.secondary.cache.index.hits COUNT : 0
rocksdb.secondary.cache.data.hits COUNT : 0
rocksdb.table.open.prefetch.tail.miss COUNT : 169
rocksdb.table.open.prefetch.tail.hit COUNT : 3607
rocksdb.db.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.micros P50 : 458.804148 P95 : 824.380799 P99 : 4391.607872 P100 : 2216301.000000 COUNT : 95999997 SUM : 51542847197
rocksdb.compaction.times.micros P50 : 92000000.000000 P95 : 107433899.000000 P99 : 107433899.000000 P100 : 107433899.000000 COUNT : 176 SUM : 14160438256
rocksdb.compaction.times.cpu_micros P50 : 45136709.000000 P95 : 48286857.142857 P99 : 48930514.285714 P100 : 56077423.000000 COUNT : 176 SUM : 8068510290
rocksdb.subcompaction.setup.times.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.sync.micros P50 : 4643414.634146 P95 : 6459219.512195 P99 : 8972800.000000 P100 : 9272054.000000 COUNT : 752 SUM : 3255669429
rocksdb.compaction.outfile.sync.micros P50 : 1229166.666667 P95 : 7537142.857143 P99 : 8655205.000000 P100 : 8655205.000000 COUNT : 192 SUM : 344661870
rocksdb.wal.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.manifest.file.sync.micros P50 : 75472.972973 P95 : 1970689.655172 P99 : 2879655.172414 P100 : 5973652.000000 COUNT : 659 SUM : 300974697
rocksdb.table.open.io.micros P50 : 3798.916968 P95 : 136400.000000 P99 : 353694.117647 P100 : 898727.000000 COUNT : 944 SUM : 21060430
rocksdb.db.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.block.compaction.micros P50 : 482.397907 P95 : 775.548370 P99 : 861.642066 P100 : 788582.000000 COUNT : 19725283 SUM : 10341199237
rocksdb.read.block.get.micros P50 : 1112.041237 P95 : 5880.975610 P99 : 6556.357724 P100 : 10919.000000 COUNT : 944 SUM : 1664432
rocksdb.write.raw.block.micros P50 : 20.956107 P95 : 33.766861 P99 : 53.969893 P100 : 504662.000000 COUNT : 32939923 SUM : 1024691223
rocksdb.numfiles.in.singlecompaction P50 : 4.000000 P95 : 4.000000 P99 : 4.000000 P100 : 5.000000 COUNT : 184 SUM : 737
rocksdb.db.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.stall P50 : 2207.224872 P95 : 4388.125522 P99 : 6780.708597 P100 : 2212763.000000 COUNT : 138149 SUM : 256848976
rocksdb.sst.read.micros P50 : 436.178066 P95 : 654.284026 P99 : 845.103777 P100 : 894548.000000 COUNT : 19726403 SUM : 9484159852
rocksdb.num.subcompactions.scheduled P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.read P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.write P50 : 3296.175187 P95 : 12841.279735 P99 : 18095.124806 P100 : 33452.000000 COUNT : 22107996 SUM : 100573299175
rocksdb.bytes.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.compressed P50 : 3650.010953 P95 : 4325.039788 P99 : 4385.042351 P100 : 1387664.000000 COUNT : 32938038 SUM : 138822808878
rocksdb.bytes.decompressed P50 : 3650.023118 P95 : 4325.066989 P99 : 4385.070888 P100 : 1387664.000000 COUNT : 19726243 SUM : 83397973465
rocksdb.compression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.decompression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.num.merge_operands P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.key.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.value.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.next.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.prev.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.read.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.compression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.decompression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.flush.micros P50 : 8824042.553191 P95 : 13333670.886076 P99 : 13848127.000000 P100 : 13848127.000000 COUNT : 752 SUM : 6851608533
rocksdb.sst.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.index.and.filter.blocks.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.sst.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.error.handler.autoresume.retry.count P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.read.bytes P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.poll.wait.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.prefetched.bytes.discarded P50 : 100.041420 P95 : 1031.000000 P99 : 1031.000000 P100 : 1031.000000 COUNT : 1649 SUM : 812792
rocksdb.multiget.io.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.level.read.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.prefetch.abort.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.open.prefetch.tail.read.bytes P50 : 352256.000000 P95 : 373630.965005 P99 : 378836.479321 P100 : 528384.000000 COUNT : 944 SUM : 334262272

140089202867200:::StartToTransferRemoteMemTable 
140079997634304:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140080584828672:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140079989241600:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140080610006784:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140080601614080:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140079980848896:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140086935025408:::Construct traditional flush job 
140089202867200:::StartToTransferRemoteMemTable 
140080626792192:::Construct traditional flush job 
Destroying HdfsFileSystem(hdfs://hdfs-storage-node:9000/)
```

```shell
# 4 thread local and 32 thread remote flush
./db_bench --db=/tmp/rrtest --num=3000000 --max_background_jobs=4 --max_write_buffer_number=4 \
--min_write_buffer_number_to_merge=2 --benchmarks=fillrandom --key_size=16 --value_size=1024 \
--disable_wal=1 --write_buffer_size=$(expr 64 \* 1048576) --threads=$(nproc) --num_column_families=8 \
--memnode_heartbeat_port=10086 --report_fillrandom_latency_and_load=true --track_flush_compaction_stats=true \
--statistics=true --use_remote_flush=1 --memnode_ip=10.10.1.5 --memnode_port=9091 --local_ip=10.10.1.1 \
--level0_slowdown_writes_trigger=100000 --level0_stop_writes_trigger=1000000 &> log.log

fillrandom   :     479.662 micros/op 66665 ops/sec 1440.032 seconds 96000000 operations;   66.1 MB/s
STATISTICS:
rocksdb.block.cache.miss COUNT : 7480127
rocksdb.block.cache.hit COUNT : 0
rocksdb.block.cache.add COUNT : 0
rocksdb.block.cache.add.failures COUNT : 0
rocksdb.block.cache.index.miss COUNT : 0
rocksdb.block.cache.index.hit COUNT : 0
rocksdb.block.cache.index.add COUNT : 0
rocksdb.block.cache.index.bytes.insert COUNT : 0
rocksdb.block.cache.filter.miss COUNT : 0
rocksdb.block.cache.filter.hit COUNT : 0
rocksdb.block.cache.filter.add COUNT : 0
rocksdb.block.cache.filter.bytes.insert COUNT : 0
rocksdb.block.cache.data.miss COUNT : 7480129
rocksdb.block.cache.data.hit COUNT : 0
rocksdb.block.cache.data.add COUNT : 0
rocksdb.block.cache.data.bytes.insert COUNT : 0
rocksdb.block.cache.bytes.read COUNT : 0
rocksdb.block.cache.bytes.write COUNT : 0
rocksdb.bloom.filter.useful COUNT : 0
rocksdb.bloom.filter.full.positive COUNT : 0
rocksdb.bloom.filter.full.true.positive COUNT : 0
rocksdb.persistent.cache.hit COUNT : 0
rocksdb.persistent.cache.miss COUNT : 0
rocksdb.sim.block.cache.hit COUNT : 0
rocksdb.sim.block.cache.miss COUNT : 0
rocksdb.memtable.hit COUNT : 0
rocksdb.memtable.miss COUNT : 0
rocksdb.l0.hit COUNT : 0
rocksdb.l1.hit COUNT : 0
rocksdb.l2andup.hit COUNT : 0
rocksdb.compaction.key.drop.new COUNT : 25108031
rocksdb.compaction.key.drop.obsolete COUNT : 0
rocksdb.compaction.key.drop.range_del COUNT : 0
rocksdb.compaction.key.drop.user COUNT : 0
rocksdb.compaction.range_del.drop.obsolete COUNT : 0
rocksdb.compaction.optimized.del.drop.obsolete COUNT : 0
rocksdb.compaction.cancelled COUNT : 0
rocksdb.number.keys.written COUNT : 96000000
rocksdb.number.keys.read COUNT : 0
rocksdb.number.keys.updated COUNT : 0
rocksdb.bytes.written COUNT : 100622573330
rocksdb.bytes.read COUNT : 0
rocksdb.number.db.seek COUNT : 0
rocksdb.number.db.next COUNT : 0
rocksdb.number.db.prev COUNT : 0
rocksdb.number.db.seek.found COUNT : 0
rocksdb.number.db.next.found COUNT : 0
rocksdb.number.db.prev.found COUNT : 0
rocksdb.db.iter.bytes.read COUNT : 0
rocksdb.no.file.opens COUNT : 797
rocksdb.no.file.errors COUNT : 0
rocksdb.stall.micros COUNT : 71076804
rocksdb.db.mutex.wait.micros COUNT : 0
rocksdb.number.multiget.get COUNT : 0
rocksdb.number.multiget.keys.read COUNT : 0
rocksdb.number.multiget.bytes.read COUNT : 0
rocksdb.number.merge.failures COUNT : 0
rocksdb.bloom.filter.prefix.checked COUNT : 0
rocksdb.bloom.filter.prefix.useful COUNT : 0
rocksdb.number.reseeks.iteration COUNT : 0
rocksdb.getupdatessince.calls COUNT : 0
rocksdb.wal.synced COUNT : 0
rocksdb.wal.bytes COUNT : 0
rocksdb.write.self COUNT : 0
rocksdb.write.other COUNT : 0
rocksdb.write.wal COUNT : 0
rocksdb.compact.read.bytes COUNT : 18219768514
rocksdb.compact.write.bytes COUNT : 2901607011
rocksdb.flush.write.bytes COUNT : 116263
rocksdb.compact.read.marked.bytes COUNT : 0
rocksdb.compact.read.periodic.bytes COUNT : 0
rocksdb.compact.read.ttl.bytes COUNT : 0
rocksdb.compact.write.marked.bytes COUNT : 0
rocksdb.compact.write.periodic.bytes COUNT : 0
rocksdb.compact.write.ttl.bytes COUNT : 0
rocksdb.number.direct.load.table.properties COUNT : 0
rocksdb.number.superversion_acquires COUNT : 0
rocksdb.number.superversion_releases COUNT : 0
rocksdb.number.superversion_cleanups COUNT : 0
rocksdb.number.block.compressed COUNT : 1194745
rocksdb.number.block.decompressed COUNT : 7480924
rocksdb.number.block.not_compressed COUNT : 0
rocksdb.merge.operation.time.nanos COUNT : 0
rocksdb.filter.operation.time.nanos COUNT : 0
rocksdb.row.cache.hit COUNT : 0
rocksdb.row.cache.miss COUNT : 0
rocksdb.read.amp.estimate.useful.bytes COUNT : 0
rocksdb.read.amp.total.read.bytes COUNT : 0
rocksdb.number.rate_limiter.drains COUNT : 0
rocksdb.number.iter.skip COUNT : 0
rocksdb.blobdb.num.put COUNT : 0
rocksdb.blobdb.num.write COUNT : 0
rocksdb.blobdb.num.get COUNT : 0
rocksdb.blobdb.num.multiget COUNT : 0
rocksdb.blobdb.num.seek COUNT : 0
rocksdb.blobdb.num.next COUNT : 0
rocksdb.blobdb.num.prev COUNT : 0
rocksdb.blobdb.num.keys.written COUNT : 0
rocksdb.blobdb.num.keys.read COUNT : 0
rocksdb.blobdb.bytes.written COUNT : 0
rocksdb.blobdb.bytes.read COUNT : 0
rocksdb.blobdb.write.inlined COUNT : 0
rocksdb.blobdb.write.inlined.ttl COUNT : 0
rocksdb.blobdb.write.blob COUNT : 0
rocksdb.blobdb.write.blob.ttl COUNT : 0
rocksdb.blobdb.blob.file.bytes.written COUNT : 0
rocksdb.blobdb.blob.file.bytes.read COUNT : 0
rocksdb.blobdb.blob.file.synced COUNT : 0
rocksdb.blobdb.blob.index.expired.count COUNT : 0
rocksdb.blobdb.blob.index.expired.size COUNT : 0
rocksdb.blobdb.blob.index.evicted.count COUNT : 0
rocksdb.blobdb.blob.index.evicted.size COUNT : 0
rocksdb.blobdb.gc.num.files COUNT : 0
rocksdb.blobdb.gc.num.new.files COUNT : 0
rocksdb.blobdb.gc.failures COUNT : 0
rocksdb.blobdb.gc.num.keys.relocated COUNT : 0
rocksdb.blobdb.gc.bytes.relocated COUNT : 0
rocksdb.blobdb.fifo.num.files.evicted COUNT : 0
rocksdb.blobdb.fifo.num.keys.evicted COUNT : 0
rocksdb.blobdb.fifo.bytes.evicted COUNT : 0
rocksdb.txn.overhead.mutex.prepare COUNT : 0
rocksdb.txn.overhead.mutex.old.commit.map COUNT : 0
rocksdb.txn.overhead.duplicate.key COUNT : 0
rocksdb.txn.overhead.mutex.snapshot COUNT : 0
rocksdb.txn.get.tryagain COUNT : 0
rocksdb.number.multiget.keys.found COUNT : 0
rocksdb.num.iterator.created COUNT : 0
rocksdb.num.iterator.deleted COUNT : 0
rocksdb.block.cache.compression.dict.miss COUNT : 0
rocksdb.block.cache.compression.dict.hit COUNT : 0
rocksdb.block.cache.compression.dict.add COUNT : 0
rocksdb.block.cache.compression.dict.bytes.insert COUNT : 0
rocksdb.block.cache.add.redundant COUNT : 0
rocksdb.block.cache.index.add.redundant COUNT : 0
rocksdb.block.cache.filter.add.redundant COUNT : 0
rocksdb.block.cache.data.add.redundant COUNT : 0
rocksdb.block.cache.compression.dict.add.redundant COUNT : 0
rocksdb.files.marked.trash COUNT : 0
rocksdb.files.deleted.immediately COUNT : 182
rocksdb.error.handler.bg.errro.count COUNT : 0
rocksdb.error.handler.bg.io.errro.count COUNT : 0
rocksdb.error.handler.bg.retryable.io.errro.count COUNT : 0
rocksdb.error.handler.autoresume.count COUNT : 0
rocksdb.error.handler.autoresume.retry.total.count COUNT : 0
rocksdb.error.handler.autoresume.success.count COUNT : 0
rocksdb.memtable.payload.bytes.at.flush COUNT : 0
rocksdb.memtable.garbage.bytes.at.flush COUNT : 0
rocksdb.secondary.cache.hits COUNT : 0
rocksdb.verify_checksum.read.bytes COUNT : 0
rocksdb.backup.read.bytes COUNT : 0
rocksdb.backup.write.bytes COUNT : 0
rocksdb.remote.compact.read.bytes COUNT : 0
rocksdb.remote.compact.write.bytes COUNT : 0
rocksdb.hot.file.read.bytes COUNT : 0
rocksdb.warm.file.read.bytes COUNT : 0
rocksdb.cold.file.read.bytes COUNT : 0
rocksdb.hot.file.read.count COUNT : 0
rocksdb.warm.file.read.count COUNT : 0
rocksdb.cold.file.read.count COUNT : 0
rocksdb.last.level.read.bytes COUNT : 0
rocksdb.last.level.read.count COUNT : 0
rocksdb.non.last.level.read.bytes COUNT : 18506493691
rocksdb.non.last.level.read.count COUNT : 7480929
rocksdb.block.checksum.compute.count COUNT : 7481721
rocksdb.multiget.coroutine.count COUNT : 0
rocksdb.blobdb.cache.miss COUNT : 0
rocksdb.blobdb.cache.hit COUNT : 0
rocksdb.blobdb.cache.add COUNT : 0
rocksdb.blobdb.cache.add.failures COUNT : 0
rocksdb.blobdb.cache.bytes.read COUNT : 0
rocksdb.blobdb.cache.bytes.write COUNT : 0
rocksdb.read.async.micros COUNT : 0
rocksdb.async.read.error.count COUNT : 0
rocksdb.secondary.cache.filter.hits COUNT : 0
rocksdb.secondary.cache.index.hits COUNT : 0
rocksdb.secondary.cache.data.hits COUNT : 0
rocksdb.table.open.prefetch.tail.miss COUNT : 5
rocksdb.table.open.prefetch.tail.hit COUNT : 3183
rocksdb.db.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.micros P50 : 433.338209 P95 : 678.330144 P99 : 864.029760 P100 : 996741.000000 COUNT : 95999994 SUM : 43893521299
rocksdb.compaction.times.micros P50 : 160000000.000000 P95 : 481132310.000000 P99 : 481132310.000000 P100 : 481132310.000000 COUNT : 12 SUM : 2808535949
rocksdb.compaction.times.cpu_micros P50 : 110000000.000000 P95 : 265627011.000000 P99 : 265627011.000000 P100 : 265627011.000000 COUNT : 12 SUM : 1554294227
rocksdb.subcompaction.setup.times.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.compaction.outfile.sync.micros P50 : 237500.000000 P95 : 2083333.333333 P99 : 2730299.000000 P100 : 2730299.000000 COUNT : 49 SUM : 27040198
rocksdb.wal.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.manifest.file.sync.micros P50 : 56071.428571 P95 : 2276923.076923 P99 : 2836923.076923 P100 : 2973322.000000 COUNT : 364 SUM : 132336624
rocksdb.table.open.io.micros P50 : 2812.383178 P95 : 4794.166667 P99 : 122360.000000 P100 : 457910.000000 COUNT : 797 SUM : 4683903
rocksdb.db.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.block.compaction.micros P50 : 446.580062 P95 : 574.279949 P99 : 810.739070 P100 : 780423.000000 COUNT : 7480133 SUM : 3341633289
rocksdb.read.block.get.micros P50 : 1162.383268 P95 : 1831.917808 P99 : 2945.000000 P100 : 8177.000000 COUNT : 797 SUM : 945866
rocksdb.write.raw.block.micros P50 : 20.123659 P95 : 32.705622 P99 : 42.685163 P100 : 382235.000000 COUNT : 1194844 SUM : 34156913
rocksdb.numfiles.in.singlecompaction P50 : 18.500000 P95 : 60.000000 P99 : 60.000000 P100 : 60.000000 COUNT : 15 SUM : 355
rocksdb.db.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.write.stall P50 : 2005.380740 P95 : 4200.246090 P99 : 5476.194384 P100 : 635463.000000 COUNT : 41851 SUM : 71108100
rocksdb.sst.read.micros P50 : 362.392716 P95 : 563.809295 P99 : 748.491150 P100 : 780377.000000 COUNT : 7480938 SUM : 3042564337
rocksdb.num.subcompactions.scheduled P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.read P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.per.write P50 : 2980.412169 P95 : 10304.022327 P99 : 13674.018033 P100 : 33452.000000 COUNT : 26214313 SUM : 100622573330
rocksdb.bytes.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.bytes.compressed P50 : 3650.003139 P95 : 4325.055686 P99 : 4385.060356 P100 : 553882.000000 COUNT : 1194746 SUM : 5034503104
rocksdb.bytes.decompressed P50 : 3650.069385 P95 : 4325.150781 P99 : 4385.158017 P100 : 553882.000000 COUNT : 7480936 SUM : 31802862442
rocksdb.compression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.decompression.times.nanos P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.read.num.merge_operands P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.key.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.value.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.get.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.multiget.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.seek.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.next.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.prev.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.write.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.read.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.blob.file.sync.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.compression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.blobdb.decompression.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.db.flush.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.sst.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.index.and.filter.blocks.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.sst.read.per.level P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.error.handler.autoresume.retry.count P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.read.bytes P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.poll.wait.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.prefetched.bytes.discarded P50 : 1031.000000 P95 : 1031.000000 P99 : 1031.000000 P100 : 1031.000000 COUNT : 983 SUM : 814911
rocksdb.multiget.io.batch.size P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.num.level.read.per.multiget P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.async.prefetch.abort.micros P50 : 0.000000 P95 : 0.000000 P99 : 0.000000 P100 : 0.000000 COUNT : 0 SUM : 0
rocksdb.table.open.prefetch.tail.read.bytes P50 : 352256.000000 P95 : 373655.150754 P99 : 378861.683417 P100 : 528384.000000 COUNT : 797 SUM : 282853376
```