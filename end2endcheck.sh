#!/bin/bash
GAZELLE_MODE=fix
GAZELLE_DIRS=.
dirName=$(date +'%m-%d-%Y-%H%M%S')
mkdir -p "bench_results/latency/${dirName}/"

mkdir -p "bench_bin/"
# THROUGHPUT BENCHMARKS:
#
## UBPF
#./scion.sh stop
#bazel run //:gazelle --config=quiet -- update -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
#rm -f bin/*
#bazel build //:scion //:scion-ci
#tar -kxf bazel-bin/scion.tar -C bin
#tar -kxf bazel-bin/scion-ci.tar -C bin
#./scion.sh topology -c topology/default-rac.topo
#./scion.sh start
#sleep 30
#echo UBPF
#./bin/end2end_integration
#
## UBPF JIT
#./scion.sh stop
#bazel run //:gazelle --config=quiet -- update -build_tags=ubpfjit -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
#rm -f bin/*
#bazel build //:scion //:scion-ci --define gotags=ubpfjit
#tar -kxf bazel-bin/scion.tar -C bin
#tar -kxf bazel-bin/scion-ci.tar -C bin
#./scion.sh topology -c topology/default-rac.topo
#./scion.sh start
#sleep 30
#echo UBPFjit
#./bin/end2end_integration
#
## WASM
#./scion.sh stop
#bazel run //:gazelle --config=quiet -- update -build_tags=wa -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
#rm -f bin/*
#bazel build //:scion //:scion-ci --define gotags=wa
#tar -kxf bazel-bin/scion.tar -C bin
#tar -kxf bazel-bin/scion-ci.tar -C bin
#./scion.sh topology -c topology/default-rac-wa.topo
#./scion.sh start
#sleep 30
#echo WASM
#./bin/end2end_integration
#
## WASM OPTIMIZATIONS STRIPPED
#./scion.sh stop
#bazel run //:gazelle --config=quiet -- update -build_tags=waopt -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
#rm -f bin/*
#bazel build //:scion //:scion-ci --define gotags=waopt
#tar -kxf bazel-bin/scion.tar -C bin
#tar -kxf bazel-bin/scion-ci.tar -C bin
#./scion.sh topology -c topology/default-rac-waopt.topo
#./scion.sh start
#sleep 30
#echo WASM NO OPT
#./bin/end2end_integration
##
## NATIVE
#./scion.sh stop
#bazel run //:gazelle --config=quiet -- update -build_tags=native -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
#rm -f bin/*
#bazel build //:scion //:scion-ci --define gotags=native
#tar -kxf bazel-bin/scion.tar -C bin
#tar -kxf bazel-bin/scion-ci.tar -C bin
#./scion.sh topology -c topology/default-rac-waopt.topo
#./scion.sh start
#sleep 30
#echo NATIVE
#./bin/end2end_integration


# TIMING BENCHMARKS:

# UBPF
./scion.sh stop
bazel run //:gazelle --config=quiet -- update -build_tags=timing -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
rm -f bin/*
bazel build //:scion //:scion-ci --define gotags=timing
tar -kxf bazel-bin/scion.tar -C bin
tar -kxf bazel-bin/scion-ci.tar -C bin
./scion.sh topology -c topology/default-rac.topo
./scion.sh start
sleep 30
echo UBPF TIMING
./bin/end2end_integration

# UBPF JIT
./scion.sh stop
bazel run //:gazelle --config=quiet -- update -build_tags=ubpfjit,timing -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
rm -f bin/*
bazel build //:scion //:scion-ci --define gotags=ubpfjit,timing
tar -kxf bazel-bin/scion.tar -C bin
tar -kxf bazel-bin/scion-ci.tar -C bin
./scion.sh topology -c topology/default-rac.topo
./scion.sh start
sleep 30
echo UBPFjit TIMING
./bin/end2end_integration

# WASM
./scion.sh stop
bazel run //:gazelle --config=quiet -- update -build_tags=wa,timing -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
rm -f bin/*
bazel build //:scion //:scion-ci --define gotags=wa,timing
tar -kxf bazel-bin/scion.tar -C bin
tar -kxf bazel-bin/scion-ci.tar -C bin
./scion.sh topology -c topology/default-rac-wa.topo
./scion.sh start
sleep 30
echo WASM TIMING
./bin/end2end_integration

# WASM OPTIMIZATIONS STRIPPED
./scion.sh stop
bazel run //:gazelle --config=quiet -- update -build_tags=waopt,timing -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
rm -f bin/*
bazel build //:scion //:scion-ci --define gotags=waopt,timing
tar -kxf bazel-bin/scion.tar -C bin
tar -kxf bazel-bin/scion-ci.tar -C bin
./scion.sh topology -c topology/default-rac-waopt.topo
./scion.sh start
sleep 30
echo WASMOPT TIMING
./bin/end2end_integration

# NATIVE
./scion.sh stop
bazel run //:gazelle --config=quiet -- update -build_tags=native,timing -mode=$GAZELLE_MODE -go_naming_convention go_default_library $GAZELLE_DIRS
rm -f bin/*
bazel build //:scion //:scion-ci --define gotags=native,timing
tar -kxf bazel-bin/scion.tar -C bin
tar -kxf bazel-bin/scion-ci.tar -C bin
./scion.sh topology -c topology/default-rac-waopt.topo
./scion.sh start
sleep 30
echo NATIVE TIMING
./bin/end2end_integration
