#!/bin/zsh
ROCKSDB_ROOT_DEV=/root/code/rocksdb_remote_flush/dev/
FLAMEGRAPH_DIR=/root/code/FlameGraph/
ROCKSDB_DIR=/tmp/rrtest/
username=""
password=""
remote_host="10.12.174.42"
remote_path=""

if [ $# -ne 1 ]; then
  echo "Usage: $0 {0|1}"
  exit 1
fi

if [ $1 -ne 0 ] && [ $1 -ne 1 ]; then
  echo "Invalid argument: $1"
  echo "Usage: $0 {0|1}"
  exit 1
fi

rm -rf $ROCKSDB_DIR
mkdir $ROCKSDB_DIR
cd $ROCKSDB_ROOT_DEV

if [ $1 -eq 0 ]; then
  ./remote_flush_test_server 0 &
else
  ./remote_flush_test_server 1 &
fi
PID=$!
file_path="./remote_flush_perf$PID-$1.svg"
perf record -g -F 99 -p $PID -- sleep 300

kill $PID
echo $PID

perf script >remote_flush_perf$PID-$1.perf
# perf report remote_flush_perf$PID.data
$FLAMEGRAPH_DIR/stackcollapse-perf.pl remote_flush_perf$PID-$1.perf >remote_flush_perf$PID-$1.folded
$FLAMEGRAPH_DIR/flamegraph.pl remote_flush_perf$PID-$1.folded >remote_flush_perf$PID-$1.svg
echo "Sending file $file_path to $remote_host:$remote_path ..."
sshpass -p "$password" scp "$file_path" "$username@$remote_host:$remote_path"
