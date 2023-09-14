package storage

import (
	"context"
	"github.com/scionproto/scion/control/ifstate"
	"time"
)

type EgressDB interface {
	IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error)
	MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
}
