#!/usr/bin/zsh
sysctl -w kernel.shmmni=32768
val=$(ipcs | tail -5 | awk 'NR==1{print $2}')
val2=$(ipcs | wc -l)
echo $val
python3 reset_shm.py $val $val2
