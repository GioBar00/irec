
for direction in 'ire'; do
    for type in 'ubpf' 'ubpfjit' 'native' 'wa' 'waopt'; do
        mkdir -p "bench_results/bw${type}_${direction}/${dirName}/"
        for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768; do
            killall ${type}_bw_${direction}_rac
            killall ${type}_bw_${direction}_bench
            echo Benchmarking bw $direction $type $i
            bench_bin/${type}_bw_${direction}_bench ${i} 0 bw > "bench_results/bw${type}_${direction}/${dirName}/bench${i}.log" &
            sleep 20;
            bench_bin/${type}_bw_${direction}_rac --config "bench_conf/rac0.toml" >> "bench_results/bw${type}_${direction}/${dirName}/rac${i}.log"
        done
    	killall ${type}_bw_${direction}_rac
    	killall ${type}_bw_${direction}_bench
    done
done
