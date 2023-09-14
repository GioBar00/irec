package ingress

import (
	"context"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

// SegmentCreationServer proxies beaconing requests from legacy scion to the Irec implementation
type SegmentCreationServer struct {
	IngressServer *IngressServer
}

func (s SegmentCreationServer) Beacon(ctx context.Context,
	req *cppb.BeaconRequest) (*cppb.BeaconResponse, error) {
	_, err := s.IngressServer.Handle(ctx, &cppb.IncomingBeacon{
		Segment: req.Segment,
	})
	return &cppb.BeaconResponse{}, err
}
