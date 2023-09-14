#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
for direction in 'ire' 'irie'; do
    for type in 'ubpf' 'ubpfjit' 'native' 'wa' 'waopt'; do
        mkdir -p "bench_results/latency_${type}_${direction}/${dirName}/"
        for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768; do
            killall ${type}_lat_${direction}_rac
            killall ${type}_lat_${direction}_bench
            echo Benchmarking latency $direction $type $i
            bench_bin/${type}_lat_${direction}_bench ${i} 0 latency > "bench_results/latency_${type}_${direction}/${dirName}/bench${i}.log" &
            sleep 20;
            bench_bin/${type}_lat_${direction}_rac --config "bench_conf/rac0.toml" >> "bench_results/latency_${type}_${direction}/${dirName}/rac${i}.log"
        done
    	killall ${type}_lat_${direction}_rac
    	killall ${type}_lat_${direction}_bench
    done
done

