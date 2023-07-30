#!/bin/zsh

for i in {1..1000}; do
    ./db_bench --db=../build/mydb$i --num=1000000 --key_size=50 --value_size=100 --benchmarks=fillrandom --disable_wal=1 --max_bytes_for_level_base=4194304 --num_levels=2 --target_file_size_base=41943 --write_buffer_size=4194 --level0_file_num_compaction_trigger=10 --level0_slowdown_writes_trigger=100 --level0_stop_writes_trigger=100 &
done
