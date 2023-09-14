//go:build !timing

package ingress

import (
	"context"

	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	if err != nil {
		return &cppb.JobCompleteResponse{}, err
	}
	return &cppb.JobCompleteResponse{}, nil
}
