//go:build !wa && !waopt && !native && timing

package ingress

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/private/procperf"

	flatbuffers "github.com/google/flatbuffers/go"

	IREC "github.com/scionproto/scion/pkg/irec/includes/flatbuffers-go/irec"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) getRacJob(ctx context.Context, request *cppb.RACBeaconRequest, racJob *beacon.RacJobMetadata) (*cppb.RACJob, error) {
	//timeStart := time.Now()
	fbs, bcns, hash, rowIds, err := i.IngressDB.GetBeaconRacJob(ctx, request, racJob)
	if err != nil {
		log.Error("An error occurred when retrieving beacons from db", "err", err)
		return &cppb.RACJob{}, err
	}
	//timeDbDone := time.Now()
	// Generate random uint32 JobID
	//log.Debug("Queueing to RAC", "beacons", len(fbs))
	ret := &cppb.RACJob{
		AlgorithmHash: hash,
		Flatbuffer:    i.packBeaconsFlatbuffer(fbs),
		BeaconCount:   uint32(len(fbs)),
		BeaconsUnopt:  bcns,
		RowIds:        rowIds,
	}
	//timeEnd := time.Now()
	return ret, nil
}

func (i *IngressServer) packBeaconsFlatbuffer(fbs [][]byte) []byte {
	builder := flatbuffers.NewBuilder(0)
	fbOffsetArr := make([]flatbuffers.UOffsetT, len(fbs))

	for j, buf := range fbs {
		data := builder.CreateByteVector(buf)

		IREC.BeaconBytesWrapperStart(builder)
		IREC.BeaconBytesWrapperAddB(builder, data)
		fbOffsetArr[j] = IREC.BeaconBytesWrapperEnd(builder)
	}

	IREC.ExecutionStartBeaconsVector(builder, len(fbOffsetArr))
	for _, region := range fbOffsetArr {
		builder.PrependUOffsetT(region)
	}
	beacons := builder.EndVector(len(fbOffsetArr))

	IREC.ExecutionStartIntfsVector(builder, len(i.PropagationInterfaces))
	for _, intf := range i.PropagationInterfaces {
		IREC.CreateInterface(builder, intf)
	}
	intfs := builder.EndVector(len(i.PropagationInterfaces))

	IREC.ExecutionStart(builder)
	IREC.ExecutionAddBeacons(builder, beacons)
	IREC.ExecutionAddIntfs(builder, intfs)
	execution := IREC.ExecutionEnd(builder)

	builder.Finish(execution)

	finishedBuf := builder.FinishedBytes()
	return finishedBuf
}

func (i *IngressServer) GetBeacons(ctx context.Context, req *cppb.BeaconQuery) (*cppb.RACJob, error) {
	pp := procperf.GetNew(procperf.Retrieved, "")
	timeGenS := time.Now()
	// Generate random uint32 JobID
	jobID, err := rand.Int(rand.Reader, big.NewInt(1<<32))
	if err != nil {
		return &cppb.RACJob{}, err
	}
	timeGenE := time.Now()
	defer pp.Write()
	pp.SetID(fmt.Sprintf("%d", uint32(jobID.Uint64())))
	pp.AddDurationT(timeGenS, timeGenE) // 0
	timeDbS := time.Now()
	fbs, bcns, err := i.IngressDB.GetBeacons(ctx, req)
	if err != nil {
		return &cppb.RACJob{}, err
	}
	timeDbE := time.Now()
	pp.AddDurationT(timeDbS, timeDbE) // 1
	pp.SetNumBeacons(uint32(len(fbs)))
	timeRacJobS := time.Now()
	racJob := &cppb.RACJob{
		PropIntfs:    i.PropagationInterfaces,
		Flatbuffer:   i.packBeaconsFlatbuffer(fbs),
		BeaconCount:  uint32(len(fbs)),
		BeaconsUnopt: bcns,
		JobID:        uint32(jobID.Uint64()),
	}
	timeRacJobE := time.Now()
	pp.AddDurationT(timeRacJobS, timeRacJobE) // 2
	return racJob, nil
}
