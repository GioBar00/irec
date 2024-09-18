//go:build timing

package ingress

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/private/procperf"
	"time"

	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	timeMarkingS := time.Now()
	err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	if err != nil {
		return &cppb.JobCompleteResponse{}, err
	}
	timeMarkingE := time.Now()
	//fmt.Printf("marking=%d\n", timeMarkingE.Sub(timeMarkingS).Nanoseconds())
	if err := procperf.AddTimestampsDoneBeacon(fmt.Sprintf("%d", req.JobID), procperf.Completed, []time.Time{timeMarkingS, timeMarkingE}); err != nil {
		log.Error("PROCPERF: Error when completing job", "err", err)
	}
	return &cppb.JobCompleteResponse{}, nil
}
