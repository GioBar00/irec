package rac

import (
	"context"

	grpc2 "google.golang.org/grpc"

	"github.com/scionproto/scion/pkg/grpc"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

type EgressWriter interface {
	WriteBeacons(ctx context.Context, beacons []*cppb.EgressBeacon, jobID uint32) error
}
type Writer struct {
	Conn *grpc2.ClientConn
}

func (w *Writer) WriteBeacons(ctx context.Context, beacons []*cppb.EgressBeacon, jobID uint32) error {
	client := cppb.NewEgressIntraServiceClient(w.Conn)
	_, err := client.RequestPropagation(ctx, &cppb.PropagationRequest{
		Beacon: beacons,
		JobID:  jobID,
	}, grpc.RetryProfile...)
	return err
}
