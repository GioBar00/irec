//go:build timing

package ebpf

//#include "irec_lib.h"
import "C"
import (
	"context"
	"fmt"
	"os"
	"path"
	"time"
	"unsafe"

	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"github.com/scionproto/scion/rac"
	"github.com/scionproto/scion/rac/config"
)

type EbpfEnv struct {
	Writer    rac.EgressWriter
	Static    bool
	JIT       bool
	staticVM  *C.struct_ubpf_vm
	staticCtx *C.struct_ubpf_ctx
}

func (e *EbpfEnv) Initialize() {

}
func (e *EbpfEnv) InitStaticAlgo(alg config.RACAlgorithm) {

	code, err := os.ReadFile(path.Clean(fmt.Sprintf("%s", alg.FilePath)))
	if err != nil {
		fmt.Println("err", err)
		return
	}
	e.staticCtx = C.new_ctx()
	e.staticVM = C.create_vm(unsafe.Pointer(&code[0]), C.ulong(len(code)), e.staticCtx, C.bool(e.JIT))

}

func (e *EbpfEnv) ExecuteDynamic(ctx context.Context, job *cppb.RACJob, code []byte, counter int32) (*cppb.JobCompleteNotify, error) {
	return e.executeVM(ctx, job.Flatbuffer, job, code)
}

func (e *EbpfEnv) ExecuteStatic(ctx context.Context, job *cppb.RACJob, counter int32) (*cppb.JobCompleteNotify, error) {
	return e.executeVM(ctx, job.Flatbuffer, job, []byte{})
}

func (e *EbpfEnv) executeVM(ctx context.Context, beaconFlatbuffer []byte, job *cppb.RACJob, code []byte) (*cppb.JobCompleteNotify, error) {
	loadedTime := time.Now()
	vm := e.staticVM
	vmCtx := e.staticCtx
	if !e.Static {
		vmCtx = C.new_ctx()
		vm = C.create_vm(unsafe.Pointer(&code[0]), C.ulong(len(code)), vmCtx, C.bool(e.JIT))
	}
	prepareMemTime := time.Now()
	C.prepare_mem(vmCtx, vm, unsafe.Pointer(&beaconFlatbuffer[0]), C.ulong(len(beaconFlatbuffer)))
	execTime := time.Now()
	ret := C.exec_vm(vm, vmCtx, C.bool(e.JIT))
	var p *C.struct_beacon_result = ret.beacon
	totalExec := time.Now()
	// Write the selected beacons to the egress gateway
	selection := make([]*cppb.BeaconAndEgressIntf, 0)
	selectedBeacons := make([]*cppb.EgressBeacon, int(ret.result_len))
	for i := 0; i < int(ret.result_len); i++ {
		beacon_res := (*C.struct_beacon_result)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + uintptr(i*int(unsafe.Sizeof(*p)))))
		ei := (*C.uint)(beacon_res.egress_intfs)
		intfs := make([]uint32, int(beacon_res.egress_intfs_len))
		for j := 0; j < int(beacon_res.egress_intfs_len); j++ {
			intfs[j] = uint32(*((*C.uint)(unsafe.Pointer(uintptr(unsafe.Pointer(ei)) + uintptr(j*int(unsafe.Sizeof(*ei)))))))
		}

		selectedBeacons[i] = &cppb.EgressBeacon{
			PathSeg:         job.BeaconsUnopt[beacon_res.beacon_id].PathSeg,
			InIfId:          job.BeaconsUnopt[beacon_res.beacon_id].InIfId,
			EgressIntfs:     intfs,
			PullbasedTarget: 0,
		}

	}

	C.destroy_mem(vmCtx)
	if !e.Static {
		C.destroy_ctx(vmCtx)
		C.destroy_vm(vm)
	}

	timeEgressGrpcS := time.Now()
	err := e.Writer.WriteBeacons(ctx, selectedBeacons)
	if err != nil {
		log.Info("err", "msg", err)
		//	return &racpb.ExecutionResponse{}, selection, err
	}
	timeEgressGrpcE := time.Now()
	timeTillSubmitNoLoad := time.Since(loadedTime)

	fmt.Printf("EBPF TIME; %d, %d, %d, %d, %d\n", time.Since(totalExec), timeEgressGrpcE.Sub(timeEgressGrpcS).Nanoseconds(), timeTillSubmitNoLoad.Nanoseconds(), execTime.Sub(prepareMemTime).Nanoseconds(), totalExec.Sub(execTime).Nanoseconds())
	if e.Static {
		return &cppb.JobCompleteNotify{
			RowIDs:    []int64{},
			Selection: selection,
		}, nil
	}
	return &cppb.JobCompleteNotify{
		RowIDs:    job.RowIds,
		Selection: selection,
	}, nil
}
