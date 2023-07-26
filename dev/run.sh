#!/usr/bin/zsh
PROJECT_ROOT="$(pwd)/../"
cd $PROJECT_ROOT/dev
rm -rf trace
rm -rf ./db/*
cd $PROJECT_ROOT
mkdir build
cd $PROJECT_ROOT/build
if [[ "$1" == "1" ]]; then
    make clean
    rm -rf ./*
    rm -rf .cmake
fi
cmake ..
make rdma_server -j 18
make remote_flush_worker -j 18
make db_remote_flush_test -j 18
cd $PROJECT_ROOT/dev

cp ../build/db_remote_flush_test ./
cp ../build/remote_flush_worker ./
cp ../build/rdma_server ./
# cp ../build/shared_std_test ./
# cp ../build/memtable_list_test ./
# cp ../build/memtable_tracer_parser ./
# cp ../build/trace_memtable_test ./
# cp ../build/trace_io_test ./
# cp ../build/io_tracer_parser ./
# cp ../build/shared_memory_* ./
# cp ../build/memtable_refactor_test ./
# cp ../build/open_secondary_test ./
# cp ../build/inlineskiplist_test ./
# cp ../build/db_memtable_test ./
# cp ../build/db_test3 ./
# cp ../build/db_test4 ./

rm Log-???????????????\.log
rm -rf memtable_result/
mkdir memtable_result/
rm -rf ./data
rm -rf ./trace_memtable
rm -rf ./Log.log

chown -R $(whoami) ../
