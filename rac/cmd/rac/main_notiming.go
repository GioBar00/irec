//go:build !timing

package main

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"github.com/scionproto/scion/private/app"
	"github.com/scionproto/scion/private/topology"
	"github.com/scionproto/scion/rac"
	env2 "github.com/scionproto/scion/rac/env"
	"github.com/scionproto/scion/rac/env/ebpf"
	"github.com/scionproto/scion/rac/env/native"
	"github.com/scionproto/scion/rac/env/wasm"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/resolver"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/scionproto/scion/pkg/private/serrors"
)

func realMain(ctx context.Context) error {

	topo, err := topology.NewLoader(topology.LoaderCfg{
		File:      globalCfg.General.Topology(),
		Reload:    app.SIGHUPChannel(ctx),
		Validator: &topology.DefaultValidator{},
	})
	log.Info("config", "is", globalCfg.RAC)
	if err != nil {
		return serrors.WrapStr("creating topology loader", err)
	}
	g, errCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer log.HandlePanic()
		return topo.Run(errCtx)
	})

	dialer := &libgrpc.TCPDialer{
		SvcResolver: func(dst addr.HostSVC) []resolver.Address {
			if base := dst.Base(); base != addr.SvcCS {
				panic("Unsupported address type, implementation error?")
			}
			targets := []resolver.Address{}
			for _, entry := range topo.ControlServiceAddresses() {
				targets = append(targets, resolver.Address{Addr: entry.String()})
			}
			return targets
		},
	}

	writer := rac.Writer{
		//Conn: conn,
	}
	var env env2.Environment
	if MODE == "ubpf" {
		log.Info("Running UBPF RAC")
		env = &ebpf.EbpfEnv{Writer: &writer, Static: globalCfg.RAC.Static, JIT: false}
	} else if MODE == "ubpfjit" {
		log.Info("Running UBPF JIT RAC")
		env = &ebpf.EbpfEnv{Writer: &writer, Static: globalCfg.RAC.Static, JIT: true}
	} else if MODE == "native" {
		log.Info("Running NATIVE RAC")
		env = &native.NativeEnv{Writer: &writer, Static: globalCfg.RAC.Static}
	} else {
		log.Info("Running WASM RAC")
		env = &wasm.WasmEnv{Writer: &writer}
	}
	env.Initialize()
	if globalCfg.RAC.Static {
		env.InitStaticAlgo(globalCfg.RAC.StaticAlgorithm)
	}

	algCache := rac.AlgorithmCache{Algorithms: make(map[string][]byte)}

	ctr := atomic.Uint64{}
	// main loop
	if globalCfg.RAC.Static {
		staticLoop(ctx, dialer, algCache, env, &ctr)
	} else {
		dynamicLoop(ctx, dialer, algCache, env, &ctr)
	}
	return nil
	//return g.Wait()
}

func dynamicLoop(ctx context.Context, dialer *libgrpc.TCPDialer, algCache rac.AlgorithmCache, env env2.Environment, ctr *atomic.Uint64) {

	conn, err := dialer.DialLimit(ctx, addr.SvcCS, 200)
	if err != nil {
		log.Error("Error when retrieving job for sources", "err", err)
		time.Sleep(1 * time.Second)
		return
	}
	defer conn.Close()
	for true {
		func() {
			//timeS := time.Now()
			client := cppb.NewIngressIntraServiceClient(conn)
			// First get possible sources from the ingress gateway (source=originas, algorithmid, alghash combo)
			exec, err1 := client.GetJob(ctx, &cppb.RACBeaconRequest{IgnoreIntfGroup: false}, libgrpc.RetryProfile...)
			if err1 != nil {
				log.Error("Error when retrieving beacon job", "err", err1)
				if strings.Contains(err1.Error(), "locked") {
					return
				}
				time.Sleep(1 * time.Second)
				return
			}
			if exec.BeaconCount == 0 {
				time.Sleep(1 * time.Second)
				return
			}
			//fmt.Printf(" fetch=%d\n", time.Now().Sub(timeS).Nanoseconds())
			//timeS = time.Now()
			// If there are PCB sources to process, get the job. This will mark the PCB's as taken such that other
			// RACS do not reprocess them.
			algorithm, exists := algCache.Algorithms[string(exec.AlgorithmHash)]
			if !exists && MODE != "native" {
				algResponse, err := client.GetAlgorithm(context.Background(), &cppb.AlgorithmRequest{AlgorithmHash: exec.AlgorithmHash})
				if err != nil {
					log.Error("Error when retrieving algorithm", "err", err)
					time.Sleep(1 * time.Second)
					return
				}
				algorithm = algResponse.Code
				algCache.Algorithms[string(exec.AlgorithmHash)] = algResponse.Code

			}
			//startEbpf := time.Now()
			res, err := env.ExecuteDynamic(ctx, exec, algorithm, int32(ctr.Load()))
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(1 * time.Second)
				return
			}
			//fmt.Printf(" alg=%d\n", time.Now().Sub(timeS).Nanoseconds())
			//timeS = time.Now()
			_, err = client.JobComplete(ctx, res)
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			//fmt.Printf(" write=%d\n", time.Now().Sub(timeS).Nanoseconds())

			//endEbpf := time.Since(startEbpf)
			//startWasm := time.Now()
			//_, err3 := env.ExecuteDynamic(ctx, exec, int32(ctr.Load()))
			//endWasm := time.Since(startWasm)
			//fmt.Printf("wasm=%d, ebpf=%d\n", endEbpf.Nanoseconds(), endWasm.Nanoseconds())
			//if err3 != nil {
			//	log.Error("Error when executing rac for sources", "err", err3)
			//	time.Sleep(1 * time.Second)
			//	continue
			//}
			ctr.Add(1)

		}()
	}
}

func staticLoop(ctx context.Context, dialer *libgrpc.TCPDialer, algCache rac.AlgorithmCache, env env2.Environment, ctr *atomic.Uint64) {

	conn, err := dialer.DialLimit(ctx, addr.SvcCS, 50)

	if err != nil {
		log.Error("Error when retrieving job for sources", "err", err)
		time.Sleep(100 * time.Millisecond)
		return
	}
	defer conn.Close()
	for true {
		func() {
			client := cppb.NewIngressIntraServiceClient(conn)
			exec, err2 := client.GetBeacons(ctx, &cppb.BeaconQuery{})
			if err2 != nil {
				log.Error("Error when retrieving job for sources", "err", err2)
				time.Sleep(100 * time.Millisecond)
				return
			}
			//startEbpf := time.Now()
			log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
			res, err := env.ExecuteStatic(ctx, exec, int32(ctr.Load()))

			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			_, err = client.JobComplete(ctx, res)
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			ctr.Add(1)
			time.Sleep(2000 * time.Millisecond)
		}()
	}
}
