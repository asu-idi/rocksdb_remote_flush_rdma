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
make rdma_server -j $(nproc)
make remote_flush_worker -j $(nproc)
make db_remote_flush_test -j $(nproc)
make remote_flush_test_server -j $(nproc)
make rdma_server -j $(nproc)
make tcp_server -j $(nproc)
make db_bench -j $(nproc)

cd $PROJECT_ROOT/dev

cp ../build/rdma_server ./
cp ../build/tcp_server ./
cp ../build/db_remote_flush_test ./
cp ../build/remote_flush_worker ./
cp ../build/rdma_server ./
cp ../build/remote_flush_test_server ./
cp ../build/db_bench ./

rm Log-*
rm -rf memtable_result/
mkdir memtable_result/
rm -rf ./data
rm -rf ./trace_memtable
rm -rf ./Log.log

chown -R $(whoami) ../
