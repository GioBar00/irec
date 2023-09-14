//go:build !timing

package ingress

import (
	"context"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
)

func (i *IngressServer) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	if err != nil {
		return &cppb.JobCompleteResponse{}, err
	}
	if len(req.Selection) <= 0 || len(req.Selection) >= maximumSelectableBeacons {
		return &cppb.JobCompleteResponse{}, nil
	}

	go func() {
		defer log.HandlePanic()
		req := req
		senderCtx, cancelF := context.WithTimeout(context.Background(), defaultNewSenderTimeout)
		defer cancelF()
		beacons := make([]*cppb.EgressBeacon, 0)
		for _, selected := range req.Selection {
			bcn, err := i.IngressDB.GetBeaconByRowID(senderCtx, selected.Selected)
			if err != nil {
				log.Error("error occurred", "err", err)
				return
			}
			bcn.EgressIntfs = selected.EgressIntfs
			beacons = append(beacons, bcn)
		}

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
	}()

	return &cppb.JobCompleteResponse{}, nil
}
