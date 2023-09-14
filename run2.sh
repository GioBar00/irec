#!/bin/bash
killall rac
echo "Launching background jobs ..."
for i in $(seq 0 $1) ; do
    taskset -c ${i} bin/rac --config "bench_conf/rac${i}.toml" &
done


