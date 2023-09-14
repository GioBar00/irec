package egress

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/control/irec/egress/storage"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/segment/extensions/irec"
	"github.com/scionproto/scion/pkg/snet/addrutil"
	"github.com/scionproto/scion/private/topology"
)

// The propagator takes beacons from the beaconDb,
// Propagator checks the egress db, if not present for intf, extend and send immediately.
// It also keeps track when last sent beacon, such that an originate script can send beacon if necessary.

type Propagator struct {
	Store storage.EgressDB
	//Tick                  Tick
	Dialer                grpc.Dialer
	Core                  bool
	Pather                *addrutil.Pather
	Local                 addr.IA
	AllInterfaces         *ifstate.Interfaces
	PropagationInterfaces func() []*ifstate.Interface
	Interfaces            map[uint32]*ifstate.Interface
	Extender              Extender
	PropagationFilter     func(*ifstate.Interface) bool
	Peers                 []uint16
	SenderFactory         SenderFactory
	Writers               []Writer
	Originator            *PullBasedOriginator
}

func (p *Propagator) RequestPullBasedOrigination(ctx context.Context, request *cppb.PullPathsRequest) (*cppb.PropagationRequestResponse, error) {
	if p.Core {
		p.Originator.OriginatePullBasedBeacon(ctx, OriginationAlgorithm{
			ID:   request.AlgorithmId,
			Hash: request.AlgorithmHash,
		}, addr.IA(request.DestinationIsdAs), 0, 30*time.Second, 0)

	}
	return &cppb.PropagationRequestResponse{}, nil
}

func HashBeacon(segment *seg.PathSegment) []byte {
	h := sha256.New()
	binary.Write(h, binary.BigEndian, segment.Info.SegmentID)
	binary.Write(h, binary.BigEndian, segment.Info.Timestamp)
	binary.Write(h, binary.BigEndian, segment.Info.Raw)
	for _, ase := range segment.ASEntries {
		binary.Write(h, binary.BigEndian, ase.Local)
		if ase.Extensions.Irec != nil {
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.AlgorithmHash)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.AlgorithmId)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.InterfaceGroup)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.PullBased)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.PullBasedHyperPeriod)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.PullBasedPeriod)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.PullBasedMinBeacons)
			binary.Write(h, binary.BigEndian, ase.Extensions.Irec.PullBasedTarget)
		}
		binary.Write(h, binary.BigEndian, ase.HopEntry.HopField.ConsIngress)
		binary.Write(h, binary.BigEndian, ase.HopEntry.HopField.ConsEgress)
		for _, peer := range ase.PeerEntries {
			binary.Write(h, binary.BigEndian, peer.Peer)
			binary.Write(h, binary.BigEndian, peer.HopField.ConsIngress)
			binary.Write(h, binary.BigEndian, peer.HopField.ConsEgress)
		}
	}
	return h.Sum(nil)
}

const defaultNewSenderTimeout = 10 * time.Second

func (p *Propagator) RequestPropagation(ctx context.Context, request *cppb.PropagationRequest) (*cppb.PropagationRequestResponse, error) {
	var wg sync.WaitGroup
	for _, bcn := range request.Beacon {
		// If the beacon is a pull-based beacon, handle the beacon separately.
		segment, err := seg.BeaconFromPB(bcn.PathSeg)

		if err != nil {
			log.Error("Could not parse beacon segment", "err", err)
			continue
		}

		if addr.IA(bcn.PullbasedTarget).Equal(p.Local) || (segment.ASEntries[0].Extensions.Irec != nil && segment.ASEntries[0].Extensions.Irec.PullBasedTarget.Equal(p.Local)) {
			log.Debug("Pull based beacon for this AS.")
			err := p.HandlePullBasedRequest(ctx, bcn)
			if err != nil {
				log.Error("Error occurred during processing of pull-based beacon targeted at this AS", "err", err)
				continue
			}
			continue
		}

		// Write the beacons to path servers in a seperate goroutine
		// TODO(jvb); ALternatively, we can write these to the egress database and use a periodic writer to write to the path servers.
		// A non-core AS can have multiple writers, core only one, write to all:
		for _, writer := range p.Writers {
			if writer.WriterType() == seg.TypeCoreR {
				continue
			}
			wg.Add(1)
			bcn := bcn
			writer := writer
			go func() {
				defer log.HandlePanic()
				defer wg.Done()
				segment, err := seg.BeaconFromPB(bcn.PathSeg)

				if err != nil {
					log.Error("Could not parse beacon segment", "err", err)
					return
				}
				// writer has side-effects for beacon, therefore recreate beacon arr for each writer

				log.Info("NOTIF; writing", "bcn", segment)
				err = writer.Write(context.Background(), []beacon.Beacon{{Segment: segment, InIfId: uint16(bcn.InIfId)}}, p.Peers, true)
				if err != nil {
					log.Error("Could not write beacon to path servers", "err", err)
				}
			}()
		}

		// Every beacon is to be propagated on a set of interfaces
		for _, intfId := range bcn.EgressIntfs {
			wg.Add(1)
			// Copy to have the vars in the goroutine
			intfId := intfId
			bcn := bcn
			go func() {
				defer log.HandlePanic()
				defer wg.Done()
				intf := p.Interfaces[intfId]
				if intf == nil {
					log.Error("Attempt to send beacon on non-existent interface", "egress_interface", intfId)
					return
				}
				if !p.PropagationFilter(intf) {
					log.Error("Attempt to send beacon on filtered egress interface", "egress_interface", intfId)
					return
				}

				segment, err := seg.BeaconFromPB(bcn.PathSeg)
				if err != nil {
					log.Error("Beacon DB propagation segment failed", "err", err)
					return
				}

				if p.shouldIgnore(segment, intf) {
					return
				}
				beaconHash := HashBeacon(segment)

				log.Info("Irec Propagation requested for", hex.EncodeToString(beaconHash), segment)
				// Check if beacon is already propagated before using egress db
				propagated, _, err := p.Store.IsBeaconAlreadyPropagated(ctx, beaconHash, intf)
				if err != nil {
					log.Error("Beacon DB Propagation check failed", "err", err)
					return
				}
				// If so, don't propagate
				if propagated {
					log.Info("Beacon is known in egress database, skipping propagation.")
					return
				}
				// If not, mark it as propagated
				err = p.Store.MarkBeaconAsPropagated(ctx, beaconHash, intf, time.Now().Add(time.Hour))
				if err != nil {
					log.Error("Beacon DB Propagation add failed", "err", err)
					return
				}
				// If the Origin-AS used Irec, we copy the algorithmID and hash from the first as entry
				peers := SortedIntfs(p.AllInterfaces, topology.Peer)
				if segment.ASEntries[0].Extensions.Irec != nil {
					err = p.Extender.Extend(ctx, segment, uint16(bcn.InIfId),
						intf.TopoInfo().ID, true,
						&irec.Irec{
							AlgorithmHash:  segment.ASEntries[0].Extensions.Irec.AlgorithmHash,
							InterfaceGroup: 0,
							AlgorithmId:    segment.ASEntries[0].Extensions.Irec.AlgorithmId,
						},
						peers)
				} else {
					// Otherwise, default values.
					err = p.Extender.Extend(ctx, segment, uint16(bcn.InIfId),
						intf.TopoInfo().ID, false, nil, peers)
				}

				if err != nil {
					log.Error("Extending failed", "err", err)
					return
				}

				//	Propagate to ingress gateway
				senderCtx, cancel := context.WithTimeout(ctx, defaultNewSenderTimeout)
				defer cancel()
				//TODO(jvb): Check if it would be better to reuse the senders from a pool.
				sender, err := p.SenderFactory.NewSender(
					senderCtx,
					intf.TopoInfo().IA,
					intf.TopoInfo().ID,
					intf.TopoInfo().InternalAddr.UDPAddr(),
				)
				if err != nil {
					log.Error("Creating sender failed", "err", err)
					return
				}
				defer sender.Close()
				if err := sender.Send(ctx, segment); err != nil {
					log.Error("Sending beacon failed", "err", err)
					return
				}
				// Here we keep track of the last time a beacon has been sent on an interface per algorithm hash.
				// Such that the egress gateway can plan origination scripts for those algorithms that need origination.
				if segment.ASEntries[0].Extensions.Irec != nil {
					intf.Propagate(time.Now(), HashToString(segment.ASEntries[0].Extensions.Irec.AlgorithmHash))
				} else {
					intf.Propagate(time.Now(), "")
				}
			}()
		}
	}
	wg.Wait() // Necessary for tests, but possible optimization is not waiting for this.
	return &cppb.PropagationRequestResponse{}, nil
}

// shouldIgnore indicates whether a beacon should not be sent on the egress
// interface because it creates a loop.
func (p *Propagator) shouldIgnore(bseg *seg.PathSegment, intf *ifstate.Interface) bool {
	if err := beacon.FilterLoopSeg(bseg, intf.TopoInfo().IA, false); err != nil {
		return true
	}
	return false
}

func HashToString(hash []byte) string {
	return hex.EncodeToString(hash)
}
