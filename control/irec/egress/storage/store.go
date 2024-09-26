package storage

import (
	"context"
	"time"

	"github.com/scionproto/scion/control/ifstate"
)

type EgressDB interface {
	IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error)
	MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
	BeaconsThatShouldBePropagated(ctx context.Context, beacons []EgressBeacon) ([]EgressBeacon, error)
	UpdateExpiry(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
}

type EgressBeacon struct {
	Index       int
	BeaconHash  []byte
	EgressIntfs []uint32
}
