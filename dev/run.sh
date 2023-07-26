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
make remote_flush_worker -j 18
make db_remote_flush_test -j 18
cd $PROJECT_ROOT/dev

cp ../build/db_remote_flush_test ./
cp ../build/remote_flush_worker ./

rm Log-*
rm -rf memtable_result/
mkdir memtable_result/
rm -rf ./data
rm -rf ./trace_memtable
rm -rf ./Log.log

chown -R $(whoami) ../
