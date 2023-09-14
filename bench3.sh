#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/scionbench/${dirName}/"
#
for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    killall rac
    killall bench
    bin/bench ${i} scion 1000 > "bench_results/scionbench/${dirName}/${i}.log"
done




