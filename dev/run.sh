#!/usr/bin/zsh
PROJECT_ROOT="$(pwd)/../"
cd $PROJECT_ROOT/dev
rm -rf trace
rm -rf ./db/*
cd $PROJECT_ROOT
mkdir build
cd $PROJECT_ROOT/build
cmake ..
if [[ "$1" == "1" ]]; then
    make clean
fi
make all -j 18
cd $PROJECT_ROOT/dev

cp ../build/trace_analyzer ./
cp ../build/trace_query_test ./
cp ../build/flush_job_test ./
cp ../build/shared_std_test ./
cp ../build/memtable_list_test ./
# cp ../build/memtable_tracer_parser ./
# cp ../build/trace_memtable_test ./
cp ../build/trace_io_test ./
cp ../build/io_tracer_parser ./
cp ../build/shared_memory_* ./
cp ../build/memtable_refactor_test ./
cp ../build/open_secondary_test ./
cp ../build/inlineskiplist_test ./
cp ../build/db_memtable_test ./
cp ../build/db_test3 ./
cp ../build/db_test4 ./

rm -rf memtable_result/
mkdir memtable_result/
rm -rf ./data
rm -rf ./trace_memtable
rm -rf ./Log.log

chown -R $(whoami) ../
# ./trace_block_cache_test
# ./io_tracer_parser -io_trace_file $PROJECT_ROOT/dev/trace
# # ./trace_analyzer \
# #   -analyze_get \
# #   -analyze_put \
# #   -analyze_merge \
# #   -analyze_iterator \
# #   -output_access_count_stats \
# #   -output_dir=/data/rocksdb/dev/result/ \
# #   -output_key_stats \
# #   -output_qps_stats \
# #   -convert_to_human_readable_trace \
# #   -output_value_distribution \
# #   -output_key_distribution \
# #   -print_overall_stats \
# #   -print_top_k_access=3 \
# #   -output_prefix=test \
# #   -trace_path=/data/rocksdb/dev/trace
