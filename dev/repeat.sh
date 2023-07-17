#!/usr/bin/zsh

for i in {1..10}; do
    ./db_test &>Log-$i.log
    ./auto.sh
done
