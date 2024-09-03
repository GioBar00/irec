package egress

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/private/procperf"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/segment/extensions/irec"
)

// Once a pull based beacon comes in, this function handles the processing of this beacon. It reverses the path in the
// beacon and contacts the origin AS
func (h Propagator) HandlePullBasedRequest(ctx context.Context, bcn *cppb.EgressBeacon) error {

	segCopy, err := seg.BeaconFromPB(bcn.PathSeg)
	if err != nil {
		return serrors.WrapStr("Parsing pull-based beacon failed; ", err)
	}
	bcnId := fmt.Sprintf("%s %x", segCopy.GetLoggingID(), segCopy.Info.SegmentID)
	startTime := time.Now()
	if segCopy.ASEntries[0].Extensions.Irec == nil {
		return serrors.New("Beacon is not an IREC beacon")
	}
	// Extend the received beacon, as the origin AS needs our AS entry in the beacon.
	if err = h.Extender.Extend(ctx, segCopy, uint16(bcn.InIfId),
		0, true, &irec.Irec{
			AlgorithmHash:  segCopy.ASEntries[0].Extensions.Irec.AlgorithmHash,
			AlgorithmId:    segCopy.ASEntries[0].Extensions.Irec.AlgorithmId,
			InterfaceGroup: 0,
		}, h.Peers); err != nil {
		return err
	}

	address, err := h.Pather.GetPath(addr.SvcCS, segCopy)
	if err != nil {
		log.Error("Unable to choose server", "err", err)
	}
	conn, err := h.Dialer.Dial(ctx, address)
	if err != nil {
		return serrors.WrapStr("Error occurred while dialing origin AS", err)
	}
	log.Debug("Pullbased beacon, path to origin AS", "paths", address.Path)

	defer conn.Close()
	client := cppb.NewEgressInterServiceClient(conn)

	_, err = client.PullBasedCallback(ctx, &cppb.IncomingBeacon{Segment: seg.PathSegmentToPB(segCopy)})
	if err != nil {
		return err
	}
	t := time.Now()
	if err := procperf.AddTimeDoneBeacon(bcnId, procperf.Propagated, startTime, t, fmt.Sprintf("%s %x", segCopy.GetLoggingID(), segCopy.Info.SegmentID)); err != nil {
		return serrors.WrapStr("PROCPERF: error done propagate Pullbased", err)
	}
	return nil
}

// Called by other AS'es to signal that the pull based beacon has arrived and is processed.
func (h Propagator) PullBasedCallback(ctx context.Context, bcn *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	log.Debug("Pullbased request has been answered", "bcn", bcn)
	// Write the beacon to the path server;
	go func() {
		defer log.HandlePanic()
		// A non-core AS can have multiple writers, core only one, write to all:
		for _, writer := range h.Writers {
			if writer.WriterType() == seg.TypeCoreR { // 'Hack' to support reversed core segments
				segment, err := seg.SegmentFromPB(bcn.Segment)
				log.Info("pull-based path: ", "seg", segment)
				if err != nil {
					log.Error("error occurred", "err", err)
					continue
				}
				// writer has side-effects for beacon, therefore recreate beacon arr for each writer
				err = writer.Write(context.Background(), []beacon.Beacon{{Segment: segment,
					InIfID: 0}}, h.Peers, false)
				if err != nil {
					log.Error("error occurred", "err", err)
				}
			}

		}
	}()
	return &cppb.IncomingBeaconResponse{}, nil
}
