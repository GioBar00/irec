#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/latency/${dirName}/"
make gazelle
make bazel
for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    killall rac
    killall bench
    bin/bench ${i} 0 latency > "bench_results/latency/${dirName}/bench${i}_ubpf.log" &
    bin/rac --config "bench_conf/rac0.toml" >> "bench_results/latency/${dirName}/rac${i}_ubpf.log"
done

make gazelle-wa
make bazel-wa
for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    killall rac
    killall bench
    bin/bench ${i} 0 latency > "bench_results/latency/${dirName}/bench${i}_wa.log" &
    bin/rac --config "bench_conf/rac0.toml" >> "bench_results/latency/${dirName}/rac${i}_wa.log"
done

make gazelle-waopt
make bazel-waopt
for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 ; do
    killall rac
    killall bench
    bin/bench ${i} 0 latency > "bench_results/latency/${dirName}/bench${i}_waopt.log" &
    bin/rac --config "bench_conf/rac0.toml" >> "bench_results/latency/${dirName}/rac${i}_waopt.log"
done
