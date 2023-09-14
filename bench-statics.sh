#!/bin/bash
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/maxthr/${dirName}/"
for direction in 'ire' 'irie'; do
    for rac in 'static' 'dynamic'; do
        for type in 'ubpf' 'wa' 'waopt' 'native'; do
            for setSize  in 1 16 256 4096 65536; do
               bench_bin/${type}_thr_${direction}_bench ${setSize} ${setSize} max ${rac} n> "bench_results/maxthr/${dirName}/${type}-${rac}-${direction}-${setsize}-bench.log"
               killall -w ${type}_thr_${direction}_bench
            done
        done
        bench_bin/ubpfjit_thr_${direction}_bench ${setSize} ${setSize} max ${rac} j> "bench_results/maxthr/${dirName}/ubpfjit-${rac}-${direction}-${setsize}-bench.log"
        killall -w ubpfjit_thr_${direction}_bench
    done
done


