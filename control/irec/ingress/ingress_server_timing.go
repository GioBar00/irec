//go:build timing

package ingress

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/private/procperf"
	"time"

	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	pp := procperf.GetNew(procperf.Completed, fmt.Sprintf("%d", req.JobID))
	pp.SetNumBeacons(uint32(len(req.RowIDs)))
	timeMarkingS := time.Now()
	err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	if err != nil {
		return &cppb.JobCompleteResponse{}, err
	}
	timeMarkingE := time.Now()
	//fmt.Printf("marking=%d\n", timeMarkingE.Sub(timeMarkingS).Nanoseconds())
	pp.AddDurationT(timeMarkingS, timeMarkingE)
	pp.Write()
	return &cppb.JobCompleteResponse{}, nil
}
