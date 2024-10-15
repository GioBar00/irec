package storage

import (
	"context"
	"time"

	"github.com/scionproto/scion/control/ifstate"
)

type EgressDB interface {
	IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error)
	MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
	UpdateExpiry(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
	DeleteBeacon(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) error
	BeaconsThatShouldBePropagated(ctx context.Context, beacons []EgressBeacon, expiry time.Time) ([]EgressBeacon, error)
	GetDBSize(ctx context.Context) (int, error)
}

type EgressBeacon struct {
	Index       int
	BeaconHash  *[]byte
	EgressIntfs []uint32
}
