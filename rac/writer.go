package rac

import (
	"context"
	"github.com/scionproto/scion/pkg/grpc"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	grpc2 "google.golang.org/grpc"
)

type EgressWriter interface {
	WriteBeacons(ctx context.Context, beacons []*cppb.EgressBeacon) error
}
type Writer struct {
	Conn *grpc2.ClientConn
}

func (w *Writer) WriteBeacons(ctx context.Context, beacons []*cppb.EgressBeacon) error {
	client := cppb.NewEgressIntraServiceClient(w.Conn)
	_, err := client.RequestPropagation(ctx, &cppb.PropagationRequest{
		Beacon: beacons,
	}, grpc.RetryProfile...)
	return err
}
