package egress

import (
	"context"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"

	"github.com/scionproto/scion/control/onehop"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
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

// PoolBeaconSenderFactory is used to store and reuse beacon senders.
type PoolBeaconSenderFactory struct {
	sync.Mutex
	// BeaconSenderFactory is used to create new beacon senders.
	BeaconSenderFactory *BeaconSenderFactory

	// beaconSenders is a map of beacon senders.
	poolBeaconSenders map[string]PoolBeaconSender
	// beaconSenderUsage is a map of the number of routines using a beacon sender.
	beaconSendersUsage map[string]uint
}

// NewSender returns a beacon sender that can be used to send beacons to a remote CS.
func (f *PoolBeaconSenderFactory) NewSender(
	ctx context.Context,
	dstIA addr.IA,
	egIfId uint16,
	nextHop *net.UDPAddr,
) (Sender, error) {
	f.Lock()
	defer f.Unlock()
	if f.poolBeaconSenders == nil {
		f.poolBeaconSenders = make(map[string]PoolBeaconSender)
		f.beaconSendersUsage = make(map[string]uint)
	}
	// Check if a beacon sender already exists for the given destination.
	key := beaconSenderID(dstIA, egIfId, nextHop)
	if pbs, ok := f.poolBeaconSenders[key]; ok {
		f.beaconSendersUsage[key]++
		return pbs, nil
	}
	// Create a new beacon sender.
	bs, err := f.BeaconSenderFactory.NewSender(ctx, dstIA, egIfId, nextHop)
	if err != nil {
		return nil, err
	}
	pbs := PoolBeaconSender{
		PoolBeaconSenderFactory: f,
		ID:                      key,
		BeaconSender:            bs.(*BeaconSender),
	}
	f.poolBeaconSenders[key] = pbs
	f.beaconSendersUsage[key] = 1
	return pbs, nil
}

type PoolBeaconSender struct {
	PoolBeaconSenderFactory *PoolBeaconSenderFactory
	ID                      string
	BeaconSender            *BeaconSender
}

// Send sends a beacon to the remote.
func (s PoolBeaconSender) Send(ctx context.Context, pseg *seg.PathSegment) error {
	return s.BeaconSender.Send(ctx, pseg)
}

// Close closes the BeaconSender and releases all underlying resources.
func (s PoolBeaconSender) Close() error {
	s.PoolBeaconSenderFactory.Lock()
	defer s.PoolBeaconSenderFactory.Unlock()
	s.PoolBeaconSenderFactory.beaconSendersUsage[s.ID]--
	if s.PoolBeaconSenderFactory.beaconSendersUsage[s.ID] == 0 {
		delete(s.PoolBeaconSenderFactory.poolBeaconSenders, s.ID)
		delete(s.PoolBeaconSenderFactory.beaconSendersUsage, s.ID)
		return s.BeaconSender.Close()
	}
	return nil
}

func beaconSenderID(dstIA addr.IA, egIfId uint16, nextHop *net.UDPAddr) string {
	return dstIA.String() + "_" + strconv.Itoa(int(egIfId)) + "_" + nextHop.String()
}
