#!/usr/bin/zsh

for i in {1..100}; do
    ../build/db_bench -key_size=8 -value_size=20 -num=100000 \
        -db=/tmp/rdb_benchmark -bloom_bits=10 -write_buffer_size=10000 \
        -target_file_size_base=100000 -target_file_size_multiplier=2 \
        -min_level_to_compress=1 -cache_size=10000 -enable_pipelined_write=true \
        -max_write_buffer_number=17 -max_background_flushes=16 -subcompactions=13 \
        -max_background_compactions=13 -disable_wal=true \
        -benchmarks=fillrandom,readwhilewriting -threads=40 -batch_size=100 &>./benchmark-report-$i.log
    ./db_bench --db=/tmp/rdb_benchmark --num_levels=6 --key_size=20 --prefix_size=20 --keys_per_prefix=0 \
        --value_size=100 --cache_size=17179 --cache_numshardbits=6 --compression_type=none --compression_ratio=1 \
        --min_level_to_compress=-1 --disable_seek_compaction=1 --write_buffer_size=134217728 --max_write_buffer_number=2 \
        --level0_file_num_compaction_trigger=8 --target_file_size_base=13421 --max_bytes_for_level_base=107372 --disable_wal=1 \
        --sync=0 --verify_checksum=0 --delete_obsolete_files_period_micros=3145728 --max_background_compactions=4 --max_background_flushes=16 \
        --level0_slowdown_writes_trigger=16 --level0_stop_writes_trigger=24 --statistics=0 --stats_per_interval=0 --stats_interval=10485 --histogram=0 \
        --mmap_write=0 --bloom_bits=10 --bloom_locality=1 --duration=7200 --benchmarks=fillrandom \
        --num=5242880 --threads=32 --benchmark_write_rate_limit=81920 --allow_concurrent_memtable_write=false -batch_size=100
    ./auto.sh
done
