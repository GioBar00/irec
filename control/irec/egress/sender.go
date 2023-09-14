package egress

import (
	"context"
	"github.com/scionproto/scion/control/onehop"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"google.golang.org/grpc"
	"net"
)

// SenderFactory can be used to create a new beacon sender.
type SenderFactory interface {
	// NewSender creates a new beacon sender to the specified ISD-AS over the given egress
	// interface. Nexthop is the internal router endpoint that owns the egress interface. The caller
	// is required to close the sender once it's not used anymore.
	NewSender(
		ctx context.Context,
		dst addr.IA,
		egress uint16,
		nexthop *net.UDPAddr,
	) (Sender, error)
}

// Sender sends beacons on an established connection.
type Sender interface {
	// Send sends the beacon on an established connection
	Send(ctx context.Context, pseg *seg.PathSegment) error
	// Close closes the resources associated with the sender. It must be invoked to avoid leaking
	// connections.
	Close() error
}

// BeaconSenderFactory can be used to create beacon senders.
type BeaconSenderFactory struct {
	// Dialer is used to dial the gRPC connection to the remote.
	Dialer libgrpc.Dialer
}

// NewSender returns a beacon sender that can be used to send beacons to a remote CS.
func (f *BeaconSenderFactory) NewSender(
	ctx context.Context,
	dstIA addr.IA,
	egIfId uint16,
	nextHop *net.UDPAddr,
) (Sender, error) {
	address := &onehop.Addr{
		IA:      dstIA,
		Egress:  egIfId,
		SVC:     addr.SvcCS,
		NextHop: nextHop,
	}
	conn, err := f.Dialer.Dial(ctx, address)
	if err != nil {
		return nil, serrors.WrapStr("dialing gRPC conn", err)
	}
	return &BeaconSender{
		Conn: conn,
	}, nil
}

type BeaconSender struct {
	Conn *grpc.ClientConn
}

// Send sends a beacon to the remote.
func (s BeaconSender) Send(ctx context.Context, pseg *seg.PathSegment) error {
	client := cppb.NewSegmentCreationServiceClient(s.Conn)
	_, err := client.Beacon(ctx,
		&cppb.BeaconRequest{
			Segment: seg.PathSegmentToPB(pseg),
		},
		libgrpc.RetryProfile...,
	)
	return err
}

// Close closes the BeaconSender and releases all underlying resources.
func (s BeaconSender) Close() error {
	return s.Conn.Close()
}
