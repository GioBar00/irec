#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/racbench/${dirName}/"
mkdir -p "bench_results/racbenchracs/${dirName}/"
#
for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    killall rac
    killall bench
    bin/bench ${i} rac 1000 > "bench_results/racbench/${dirName}/${i}.log" &
    bin/rac --config "bench_conf/rac0.toml" >> "bench_results/racbenchracs/${dirName}/rac${i}.log"
done
