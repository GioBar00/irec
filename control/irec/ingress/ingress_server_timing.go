//go:build timing

package ingress

import (
	"context"
	"fmt"
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
	fmt.Printf("marking=%d\n", timeMarkingE.Sub(timeMarkingS).Nanoseconds())
	return &cppb.JobCompleteResponse{}, nil
}
