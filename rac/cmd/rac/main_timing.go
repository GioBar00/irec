//go:build timing

package main

import (
	"context"
	"github.com/scionproto/scion/private/periodic"
	_ "net/http/pprof"
	"time"

	"github.com/scionproto/scion/pkg/snet"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/resolver"

	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	"github.com/scionproto/scion/private/app"
	"github.com/scionproto/scion/private/procperf"
	"github.com/scionproto/scion/private/topology"
	"github.com/scionproto/scion/rac"
	env2 "github.com/scionproto/scion/rac/env"
	"github.com/scionproto/scion/rac/env/ebpf"
	"github.com/scionproto/scion/rac/env/native"
	"github.com/scionproto/scion/rac/env/wasm"
)

const defaultJobInterval = 1 * time.Second

func realMain(ctx context.Context) error {
	if err := procperf.Init(); err != nil {
		return err
	}
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

	//ctr := atomic.Uint64{}
	jobExecutor := rac.JobExecutor{
		Dialer:           *dialer,
		Environment:      env,
		CandidateSetSize: uint32(globalCfg.RAC.CandidateSetSize),
		Mode:             MODE,
	}
	periodic.Start(jobExecutor.Task(globalCfg.RAC.Static), defaultJobInterval, 30*time.Second)

	g.Go(func() error {
		defer log.HandlePanic()
		<-errCtx.Done()
		return nil
	})

	return g.Wait()
}
