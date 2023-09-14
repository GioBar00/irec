//go:build timing

package ingress

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"time"
)

func (i *IngressServer) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	timeMarkingS := time.Now()
	err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	if err != nil {
		return &cppb.JobCompleteResponse{}, err
	}
	timeMarkingE := time.Now()
	//log.Debug("Selected beacons;", "bcns", req.Selection)
	if len(req.Selection) <= 0 || len(req.Selection) >= maximumSelectableBeacons {
		return &cppb.JobCompleteResponse{}, nil
	}

	timeRetrievingDbS := time.Now()
	beacons := make([]*cppb.EgressBeacon, 0)
	for _, selected := range req.Selection {
		bcn, err := i.IngressDB.GetBeaconByRowID(ctx, selected.Selected)
		if err != nil {
			return &cppb.JobCompleteResponse{}, err
		}
		bcn.EgressIntfs = selected.EgressIntfs
		beacons = append(beacons, bcn)
	}
	timeRetrievingDbE := time.Now()

	go func() {
		defer log.HandlePanic()

		timeEgressGrpcS := time.Now()
		senderCtx, cancelF := context.WithTimeout(context.Background(), defaultNewSenderTimeout)
		defer cancelF()

		conn, err := i.Dialer.DialLimit(senderCtx, addr.SvcCS, 50)
		if err != nil {
			log.Error("error occurred", "err", err)
			return
		}
		defer conn.Close()

		client := cppb.NewEgressIntraServiceClient(conn)
		_, err = client.RequestPropagation(senderCtx, &cppb.PropagationRequest{
			Beacon: beacons,
		}, grpc.RetryProfile...)
		if err != nil {
			log.Error("error occurred", "err", err)
			return
		}
		timeEgressGrpcE := time.Now()
		fmt.Printf("marking=%d, retrieving=%d, egressGrpc=%d\n", timeMarkingE.Sub(timeMarkingS).Nanoseconds(), timeRetrievingDbE.Sub(timeRetrievingDbS).Nanoseconds(), timeEgressGrpcE.Sub(timeEgressGrpcS).Nanoseconds())
	}()

	return &cppb.JobCompleteResponse{}, nil
}
