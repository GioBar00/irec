//go:build timing

package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/scionproto/scion/pkg/snet"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/resolver"

	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/private/app"
	"github.com/scionproto/scion/private/procperf"
	"github.com/scionproto/scion/private/topology"
	"github.com/scionproto/scion/rac"
	env2 "github.com/scionproto/scion/rac/env"
	"github.com/scionproto/scion/rac/env/ebpf"
	"github.com/scionproto/scion/rac/env/native"
	"github.com/scionproto/scion/rac/env/wasm"
)

const defaultSleepTime = 1 * time.Second

func realMain(ctx context.Context) error {

	procperf.Init()
	defer procperf.Close()

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
		SvcResolver: func(dst addr.SVC) []resolver.Address {
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

	conn, err := dialer.Dial(context.Background(), &snet.SVCAddr{SVC: addr.SvcCS})
	if err != nil {
		log.Error("Error occurred dialing ", "err", err)
	}

	writer := rac.Writer{
		Conn: conn,
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
}

func dynamicLoop(ctx context.Context, dialer *libgrpc.TCPDialer, algCache rac.AlgorithmCache, env env2.Environment, ctr *atomic.Uint64) {

	conn, err := dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 100)
	if err != nil {
		log.Error("Error when retrieving job for sources", "err", err)
		time.Sleep(defaultSleepTime)
		return
	}
	defer conn.Close()
	for ctr.Load() < 1000 {
		func() {
			timeGrpcIngress1S := time.Now() // 0
			client := cppb.NewIngressIntraServiceClient(conn)
			// First get possible sources from the ingress gateway (source=originas, algorithmid, alghash combo)
			exec, err1 := client.GetJob(ctx, &cppb.RACBeaconRequest{IgnoreIntfGroup: false, Maximum: uint32(globalCfg.RAC.CandidateSetSize)}, libgrpc.RetryProfile...)
			if err1 != nil {
				log.Error("Error when retrieving beacon job", "err", err1)
				time.Sleep(defaultSleepTime)
				return
			}
			timeGrpcIngress1E := time.Now() // 1
			log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
			if exec.BeaconCount == 0 {
				time.Sleep(defaultSleepTime)
				return
			}
			bcnIds := make([]string, 0)
			for _, beacon := range exec.BeaconsUnopt {
				ps, err := seg.BeaconFromPB(beacon.PathSeg)
				if err != nil {
					log.Error("Error when converting path segment", "err", err)
					time.Sleep(defaultSleepTime)
					return
				}
				bcnIds = append(bcnIds, procperf.GetFullId(ps.GetLoggingID(), ps.Info.SegmentID))
			}
			for _, bcnId := range bcnIds {
				if err := procperf.AddTimestampsDoneBeacon(bcnId, procperf.Received, []time.Time{}, fmt.Sprintf("%d", exec.JobID)); err != nil {
					log.Error("PROCPERF: Error when receiving beacon", "err", err)
				}
			}
			// If there are PCB sources to process, get the job. This will mark the PCB's as taken such that other
			// RACS do not reprocess them.
			algorithm, _ := algCache.Algorithms[string(exec.AlgorithmHash)]
			timeAlgorithmRetS := time.Now() // 2
			if MODE != "native" {
				algResponse, err := client.GetAlgorithm(context.Background(), &cppb.AlgorithmRequest{AlgorithmHash: exec.AlgorithmHash})
				if err != nil {
					log.Error("Error when retrieving algorithm", "err", err)
					time.Sleep(defaultSleepTime)
					return
				}
				algorithm = algResponse.Code
				algCache.Algorithms[string(exec.AlgorithmHash)] = algResponse.Code

			}
			timeAlgorithmRetE := time.Now() // 3
			res, err := env.ExecuteDynamic(ctx, exec, algorithm, int32(ctr.Load()))
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(defaultSleepTime)
				return
			}
			timeGrpcIngress2S := time.Now() // 4
			_, err = client.JobComplete(ctx, res)
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			timeGrpcIngress2E := time.Now() // 5

			//fmt.Printf("grpcIg1=%d, algorithmRet=%d, grpcIg2=%d\n", timeGrpcIngress1E.Sub(timeGrpcIngress1S).Nanoseconds(), timeAlgorithmRetE.Sub(timeAlgorithmRetS).Nanoseconds(), timeGrpcIngress2E.Sub(timeGrpcIngress2S).Nanoseconds())
			ctr.Add(1)
			log.Info("GetJob time", "duration", timeGrpcIngress1E.Sub(timeGrpcIngress1S).Seconds())
			log.Info("Execute time", "duration", timeGrpcIngress2S.Sub(timeAlgorithmRetE).Seconds())
			log.Info("JobComplete time", "duration", timeGrpcIngress2E.Sub(timeGrpcIngress2S).Seconds())
			if err := procperf.AddTimestampsDoneBeacon(fmt.Sprintf("%d", exec.JobID), procperf.Processed, []time.Time{timeGrpcIngress1S, timeGrpcIngress1E, timeAlgorithmRetS, timeAlgorithmRetE, timeGrpcIngress2S, timeGrpcIngress2E}); err != nil {
				log.Error("PROCPERF: Error when processing job", "err", err)
			}
		}()
	}
}

func staticLoop(ctx context.Context, dialer *libgrpc.TCPDialer, algCache rac.AlgorithmCache, env env2.Environment, ctr *atomic.Uint64) {

	conn, err := dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 50)

	if err != nil {
		log.Error("Error when retrieving job for sources", "err", err)
		time.Sleep(100 * time.Millisecond)
		return
	}
	defer conn.Close()
	for true {
		func() {
			timeGrpcIngress1S := time.Now() // 0
			client := cppb.NewIngressIntraServiceClient(conn)
			exec, err2 := client.GetBeacons(ctx, &cppb.BeaconQuery{Maximum: uint32(globalCfg.RAC.CandidateSetSize)})
			if err2 != nil {
				log.Error("Error when retrieving job for sources", "err", err2)
				time.Sleep(100 * time.Millisecond)
				return
			}
			timeGrpcIngress1E := time.Now() // 1
			bcnIds := make([]string, 0)
			for _, beacon := range exec.BeaconsUnopt {
				ps, err := seg.BeaconFromPB(beacon.PathSeg)
				if err != nil {
					log.Error("Error when converting path segment", "err", err)
					time.Sleep(defaultSleepTime)
					return
				}
				bcnIds = append(bcnIds, procperf.GetFullId(ps.GetLoggingID(), ps.Info.SegmentID))
			}
			for _, bcnId := range bcnIds {
				if err := procperf.AddTimestampsDoneBeacon(bcnId, procperf.Received, []time.Time{}, fmt.Sprintf("%d", exec.JobID)); err != nil {
					log.Error("PROCPERF: Error when receiving beacon", "err", err)
				}
			}
			log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
			timeExecS := time.Now() // 2
			res, err := env.ExecuteStatic(ctx, exec, int32(ctr.Load()))

			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			timeGrpcIngress2S := time.Now() // 3
			_, err = client.JobComplete(ctx, res)
			if err != nil {
				log.Error("Error when executing rac for sources", "err", err)
				time.Sleep(100 * time.Millisecond)
				return
			}
			timeGrpcIngress2E := time.Now() // 4
			ctr.Add(1)
			time.Sleep(2000 * time.Millisecond)
			if err := procperf.AddTimestampsDoneBeacon(fmt.Sprintf("%d", exec.JobID), procperf.Processed, []time.Time{timeGrpcIngress1S, timeGrpcIngress1E, timeExecS, timeGrpcIngress2S, timeGrpcIngress2E}); err != nil {
				log.Error("PROCPERF: Error when processing job", "err", err)
			}
		}()
	}
}
