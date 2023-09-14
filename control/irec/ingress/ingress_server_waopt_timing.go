//go:build (waopt || native) && timing

package ingress

import (
	"context"
	"fmt"
	"time"

	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) GetJob(ctx context.Context, request *cppb.RACBeaconRequest) (*cppb.RACJob, error) {
	timeStart := time.Now()
	bcns, hash, rowIds, err := i.IngressDB.GetBeaconJob(ctx, request.IgnoreIntfGroup)
	if err != nil {
		log.Error("An error occurred when retrieving beacons from db", "err", err)
		return &cppb.RACJob{}, err
	}
	timeDbDone := time.Now()
	//log.Debug("Queueing to RAC", "beacons", len(bcns))
	ret := &cppb.RACJob{
		BeaconsUnopt:  bcns,
		AlgorithmHash: hash,
		PropIntfs:     i.PropagationInterfaces,
		BeaconCount:   uint32(len(bcns)),
		RowIds:        rowIds,
	}
	timeEnd := time.Now()
	fmt.Printf("igdb=%d, ds=%d\n", timeDbDone.Sub(timeStart).Nanoseconds(), timeEnd.Sub(timeDbDone))
	return ret, nil
}

func (i *IngressServer) GetBeacons(ctx context.Context, req *cppb.BeaconQuery) (*cppb.RACJob, error) {
	bcns, err := i.IngressDB.GetBeacons(ctx, req)
	if err != nil {
		return &cppb.RACJob{}, err
	}
	return &cppb.RACJob{
		BeaconsUnopt: bcns,
		PropIntfs:    i.PropagationInterfaces,
		BeaconCount:  uint32(len(bcns)),
	}, nil
}
