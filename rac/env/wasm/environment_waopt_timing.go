//go:build waopt && timing

package wasm

import (
	"context"
	"fmt"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v12"
	"google.golang.org/protobuf/proto"

	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	racpb "github.com/scionproto/scion/pkg/proto/rac"
)

func (w *WasmEnv) ExecuteDynamic(ctx context.Context, job *cppb.RACJob, code []byte, counter int32) (*cppb.JobCompleteNotify, error) {
	timeLoadS := time.Now()
	dst := make([]byte, len(code))
	copy(dst, code)
	algorithm, err := wasmtime.NewModule(w.Engine, dst)
	timeLoadE := time.Now()

	execCtx, cancelF := context.WithTimeout(ctx, 1*time.Second) // Only allow an execution of max. 1 second
	defer cancelF()

	racResult, err := w.executeVM(execCtx, int32(counter), job.BeaconsUnopt, algorithm, AdditionalInfo{PropagationInterfaces: job.PropIntfs})
	if err != nil {
		return &cppb.JobCompleteNotify{}, err
	}
	timeMarshalS := time.Now()

	selectedBeacons := make([]*cppb.EgressBeacon, 0)
	for _, selected := range racResult.Selected {
		selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
			PathSeg:     job.BeaconsUnopt[selected.Id].PathSeg,
			InIfId:      job.BeaconsUnopt[selected.Id].InIfId,
			EgressIntfs: selected.EgressIntfs})

	}
	timeMarshalE := time.Now()
	timeWriteS := time.Now()
	err = w.Writer.WriteBeacons(ctx, selectedBeacons)
	if err != nil {
		log.Info("err", "msg", err)
	}
	timeWriteE := time.Now()

	fmt.Printf("wasm2: %d, %d, %d\n", timeLoadE.Sub(timeLoadS).Nanoseconds(), timeMarshalE.Sub(timeMarshalS).Nanoseconds(), timeWriteE.Sub(timeWriteS).Nanoseconds())

	return &cppb.JobCompleteNotify{
		RowIDs:    job.RowIds,
		Selection: []*cppb.BeaconAndEgressIntf{},
	}, nil

}

func (w *WasmEnv) ExecuteStatic(ctx context.Context, job *cppb.RACJob, counter int32) (*cppb.JobCompleteNotify, error) {

	execCtx, cancelF := context.WithTimeout(ctx, 1*time.Second) // Only allow an execution of max. 1 second
	defer cancelF()

	racResult, err := w.executeVM(execCtx, int32(counter), job.BeaconsUnopt, w.staticVM, AdditionalInfo{PropagationInterfaces: job.PropIntfs})
	if err != nil {
		return &cppb.JobCompleteNotify{}, err
	}

	timeDSS := time.Now()
	selectedBeacons := make([]*cppb.EgressBeacon, 0)
	for _, selected := range racResult.Selected {
		selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
			PathSeg:     job.BeaconsUnopt[selected.Id].PathSeg,
			InIfId:      job.BeaconsUnopt[selected.Id].InIfId,
			EgressIntfs: selected.EgressIntfs})

	}

	timeDSE := time.Now()

	timeEgressGrpcS := time.Now()
	err = w.Writer.WriteBeacons(ctx, selectedBeacons)
	if err != nil {
		log.Info("err", "msg", err)
	}

	timeEgressGrpcE := time.Now()

	fmt.Printf("WASMPrologue: %d, %d\n", timeDSE.Sub(timeDSS).Nanoseconds(), timeEgressGrpcE.Sub(timeEgressGrpcS).Nanoseconds())
	return &cppb.JobCompleteNotify{
		RowIDs:    []int64{},
		Selection: []*cppb.BeaconAndEgressIntf{},
	}, nil
}

func (w *WasmEnv) executeVM(ctx context.Context, count int32, beacons []*cppb.IRECBeaconUnopt, module *wasmtime.Module, info AdditionalInfo) (*racpb.RACResponse, error) {
	startModule := time.Now()
	store := wasmtime.NewStore(w.Engine) //TODO can't reuse these if we use fuel mechanism.
	linker := wasmtime.NewLinker(w.Engine)

	wasiconfig := wasmtime.NewWasiConfig()
	wasiconfig.SetEnv([]string{"WASMTIME"}, []string{"GO"})
	store.SetWasi(wasiconfig)
	if err := linker.DefineWasi(); err != nil {
		log.Error("initializing wasi env", "err", err)
		return nil, err
	}
	//hookKvFns(linker, store)
	//err := linker.DefineFunc(store, "env", "__internal_print", func(caller *wasmtime.Caller, offset int32, length int32) {
	//	if offset < 0 {
	//		log.Info("Invalid offset")
	//	}
	//	if length < 0 {
	//		log.Info("Invalid length")
	//	}
	//	mem := caller.GetExport("memory").Memory()
	//	log.Info(fmt.Sprintf("[RAC]: %s", string(mem.UnsafeData(store)[offset:offset+length])))
	//})
	//if err != nil {
	//	log.Error("hooking functions", "err", err)
	//	return
	//}
	startInstance := time.Now()
	instance1, err := linker.Instantiate(store, module)

	if err != nil {
		log.Error("instantiating module", "err", err)
		return nil, err
	}

	startJuggling := time.Now()
	irecBeacons := racpb.RACRequest{
		BeaconsUnopt: beacons,
		EgressIntfs:  info.PropagationInterfaces,
	}
	startMarshal := time.Now()
	// Now turn it into a protobuf binary message
	bytes, err := proto.Marshal(&irecBeacons)
	if err != nil {
		log.Error("marshalling beacons", "err", err)
		return nil, err
	}

	startMemory := time.Now()
	// And write to the guest memory, which will then be able to read it
	ptr, err := guestWriteBytes(instance1, store, bytes)
	if err != nil {
		log.Error("writing beacons to env", "err", err)
		return nil, err
	}

	startExec := time.Now()
	// Through the provided pointer to the bytes of the protobuf message.
	run := instance1.GetExport(store, "run").Func()
	_, err = run.Call(store, ptr, len(bytes))

	if err != nil {
		log.Error("running env", "err", err)
		return nil, err
	}
	startReadingRac := time.Now()
	// Len and ptr functions are a bypass to prevent a memory leak in WASMtime.
	length, err := instance1.GetExport(store, "len").Func().Call(store)
	if err != nil {
		log.Error("running env", "err", err)
		return nil, err
	}
	offset, err := instance1.GetExport(store, "ptr").Func().Call(store)
	if err != nil {
		log.Error("running env", "err", err)
		return nil, err
	}
	// Get the response from the memory;
	mem := instance1.GetExport(store, "memory").Memory()
	racResult := new(racpb.RACResponse)
	err = proto.Unmarshal(mem.UnsafeData(store)[offset.(int32):offset.(int32)+length.(int32)], racResult)
	if err != nil {
		log.Info("Invalid rac response")
	}
	startEgressCall := time.Now()

	fmt.Printf("%d, wasm=%d, %d, %d, %d, %d, %d, %d\n", count,
		startInstance.Sub(startModule).Nanoseconds(),
		startJuggling.Sub(startInstance).Nanoseconds(),
		startMarshal.Sub(startJuggling).Nanoseconds(),
		startMemory.Sub(startMarshal).Nanoseconds(),
		startExec.Sub(startMemory).Nanoseconds(),
		startReadingRac.Sub(startExec).Nanoseconds(),
		startEgressCall.Sub(startReadingRac).Nanoseconds())
	return racResult, nil
}
