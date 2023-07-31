#!/bin/zsh

for i in {1..1}; do
  echo "Running remote_flush test $i ..."
  ./perf.sh 1
done

for i in {1..1}; do
  echo "Running local_flush test $i ..."
  ./perf.sh 0
done

