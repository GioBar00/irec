#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/slam/${dirName}/"
for setSize  in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    for i in $(seq 1 $1) ; do
        killall rac
        killall bench
        echo "Launching background jobs ..."
        for r in $(seq 0 $((i - 1))) ; do
            taskset -c ${r} bin/rac --config "bench_conf/rac${r}.toml" >> /dev/null &
        done
        bin/bench ${setSize} slam 12 >> "bench_results/slam/${dirName}/${i}-${setSize}.log"
    done
done



