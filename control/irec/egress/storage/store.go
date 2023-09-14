package storage

import (
	"context"
	"time"

	"github.com/scionproto/scion/control/ifstate"
)

type EgressDB interface {
	IsBeaconAlreadyPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface) (bool, int, error)
	MarkBeaconAsPropagated(ctx context.Context, beaconHash []byte, intf *ifstate.Interface, expiry time.Time) error
}
