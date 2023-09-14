#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/throughput/${dirName}/"

for setSize in 16 256 4096 65536 ; do # 1 64 1024 16384  ; do
    for direction in 'irie' 'ire'; do
        for type in 'ubpf' 'ubpfjit' 'native' 'wa' 'waopt'; do
            for i in $(seq 1 $1) ; do
                killall ${type}_thr_${direction}_rac
                killall ${type}_thr_${direction}_bench
                echo "Launching background jobs ${type}, ${i}, ${setSize}"
                for r in $(seq 0 $((i - 1))) ; do
                    taskset -c ${r} bench_bin/${type}_thr_${direction}_rac --config "bench_conf/rac${r}.toml" >> /dev/null &
                done
                bench_bin/${type}_thr_${direction}_bench ${setSize} 0 throughput 7 >> "bench_results/throughput/${dirName}/${type}-${setSize}-${direction}-${i}.log"
            done
             for i in $(seq 1 $1) ; do
                killall ${type}_thr_${direction}_rac
                killall ${type}_thr_${direction}_bench
                echo "Launching background jobs, idealdb ${type}, ${i}, ${setSize}"
                for r in $(seq 0 $((i - 1))) ; do
                    taskset -c ${r} bench_bin/${type}_thr_${direction}_rac --config "bench_conf/rac${r}.toml" >> /dev/null &
                done
                bench_bin/${type}_thr_${direction}_bench ${setSize} 0 rawthr 7 >> "bench_results/throughput/${dirName}/${type}-idealdb-${setSize}-${direction}-${i}.log"
            done
            killall ${type}_thr_${direction}_rac
            killall ${type}_thr_${direction}_bench
        done
    done
done
