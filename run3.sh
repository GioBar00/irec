#!/bin/bash
make bazel
killall rac
killall bench
bin/rac --config "bench_conf/rac0.toml" &
bin/bench 4196 rac 5000
