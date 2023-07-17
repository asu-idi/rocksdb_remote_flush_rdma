#!/usr/bin/zsh

for i in {1..10}; do
    ../build/db_bench -key_size=8 -value_size=20 -num=100000 \
        -db=/tmp/rdb_benchmark -bloom_bits=10 -write_buffer_size=10000 \
        -target_file_size_base=100000 -target_file_size_multiplier=2 \
        -min_level_to_compress=1 -cache_size=10000 -enable_pipelined_write=true \
        -max_write_buffer_number=17 -max_background_flushes=16 -subcompactions=13 \
        -max_background_compactions=13 -disable_wal=true \
        -benchmarks=fillrandom,stats -threads=40 -batch_size=100 -analyze_put -statistics -report_bg_io_stats &>./benchmark-report-$i.log
    ./auto.sh
done
