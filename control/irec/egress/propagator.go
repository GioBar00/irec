package egress

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net"
	"sync"
	"time"

	"github.com/scionproto/scion/private/procperf"

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
	binary.Write(h, binary.BigEndian, segment.Info.Timestamp.UnixNano())
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
	var err error
	// convert from proto to beacon
	var pullBasedBeacons []beacon.Beacon
	var beacons []beacon.Beacon
	var beaconIndexes []int
	for i, bcn := range request.Beacon {
		segment, err := seg.BeaconFromPB(bcn.PathSeg)
		if err != nil {
			log.Error("Could not parse beacon segment", "err", err)
			continue
		}
		if addr.IA(bcn.PullbasedTarget).Equal(p.Local) || (segment.ASEntries[0].Extensions.Irec != nil && segment.ASEntries[0].Extensions.Irec.PullBasedTarget.Equal(p.Local)) {
			pullBasedBeacons = append(pullBasedBeacons, beacon.Beacon{Segment: segment, InIfID: uint16(bcn.InIfId)})
		} else {
			beacons = append(beacons, beacon.Beacon{Segment: segment, InIfID: uint16(bcn.InIfId)})
			beaconIndexes = append(beaconIndexes, i)
		}
	}
	// handle pull based beacons separately
	for _, bcn := range pullBasedBeacons {
		bcn := bcn
		wg.Add(1)
		go func() {
			defer log.HandlePanic()
			defer wg.Done()
			err := p.HandlePullBasedRequest(ctx, &bcn)
			if err != nil {
				log.Error("Error occurred during processing of pull-based beacon targeted at this AS", "err", err)
			}
		}()
	}
	// Write the beacons to path servers in a separate goroutine
	for _, writer := range p.Writers {
		if writer.WriterType() == seg.TypeCoreR {
			continue
		}
		wg.Add(1)
		beaconIndexes := beaconIndexes
		writer := writer
		go func() {
			defer log.HandlePanic()
			defer wg.Done()
			timeWriterS := time.Now()

			// make a copy of the beacons array as the writer has side effects for beacon

			beaconsCopy := make([]beacon.Beacon, len(beaconIndexes))
			// convert to proto and back to beacon to avoid side effects
			for _, i := range beaconIndexes {
				segment, err := seg.BeaconFromPB(request.Beacon[i].PathSeg)
				if err != nil {
					log.Error("Could not parse beacon segment", "err", err)
					continue
				}
				beaconsCopy[i] = beacon.Beacon{Segment: segment, InIfID: uint16(request.Beacon[i].InIfId)}
			}

			stats, err := writer.Write(context.Background(), beaconsCopy, p.Peers, true)
			if err != nil {
				log.Error("Could not write beacon to path servers", "err", err)
			}
			timeWriterE := time.Now()
			if stats.Count > 0 {
				if err := procperf.AddTimestampsDoneBeacon(writer.WriterType().String(), procperf.Written, []time.Time{timeWriterS, timeWriterE}); err != nil {
					log.Error("PROCPERF: error writing beacon", "err", err)
				}
			}
		}()
	}
	// BATCH VERSION
	//var egressBeacons []storage.EgressBeacon
	//for _, i := range beaconIndexes {
	//	egressBeacons = append(egressBeacons, storage.EgressBeacon{Index: i, BeaconHash: HashBeacon(beacons[i].Segment), EgressIntfs: request.Beacon[i].EgressIntfs})
	//}
	//egressBeacons, err := p.Store.BeaconsThatShouldBePropagated(ctx, egressBeacons)
	//if err != nil {
	//	return &cppb.PropagationRequestResponse{}, serrors.WrapStr("Could not filter beacons to be propagated", err)
	//}

	// SINGLE VERSION
	var egressBeacons []storage.EgressBeacon
	for _, i := range beaconIndexes {
		intf := p.Interfaces[uint32(beacons[i].InIfID)]
		if p.shouldIgnore(beacons[i].Segment, intf) {
			continue
		}
		beaconHash := HashBeacon(beacons[i].Segment)
		propagated, _, err := p.Store.IsBeaconAlreadyPropagated(ctx, beaconHash, p.Interfaces[uint32(beacons[i].InIfID)])
		if err != nil {
			log.Error("Beacon DB Propagation check failed", "err", err)
			continue
		}
		if !propagated {
			egressBeacons = append(egressBeacons, storage.EgressBeacon{Index: i, BeaconHash: beaconHash, EgressIntfs: request.Beacon[i].EgressIntfs})
			// pre-mark beacon as propagated in egress db with a short expiry time to avoid re-propagation and quick cleanup in case of send failure
			err = p.Store.MarkBeaconAsPropagated(ctx, beaconHash, intf, time.Now().Add(3*defaultNewSenderTimeout))
			if err != nil {
				log.Error("Beacon DB Propagation Pre-mark failed", "err", err)
				continue
			}
		}
	}

	successCh := make(chan bool)
	wg.Add(len(egressBeacons))
	for _, ebcn := range egressBeacons {
		bcn := beacons[ebcn.Index]
		beaconHash := ebcn.BeaconHash
		for _, intfId := range ebcn.EgressIntfs {
			success := false
			intfId := intfId
			bcn := bcn
			beaconHash := beaconHash
			go func() {
				defer log.HandlePanic()
				defer wg.Done()
				defer func() {
					successCh <- success
				}()

				intf := p.Interfaces[intfId]
				if intf == nil {
					log.Error("Attempt to send beacon on non-existent interface", "egress_interface", intfId)
					return
				}
				if !p.PropagationFilter(intf) {
					log.Error("Attempt to send beacon on filtered egress interface", "egress_interface", intfId)
					return
				}

				// If the Origin-AS used Irec, we copy the algorithmID and hash from the first as entry
				peers := SortedIntfs(p.AllInterfaces, topology.Peer)
				if bcn.Segment.ASEntries[0].Extensions.Irec != nil {
					err = p.Extender.Extend(ctx, bcn.Segment, bcn.InIfID,
						intf.TopoInfo().ID, true,
						&irec.Irec{
							AlgorithmHash:  bcn.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash,
							InterfaceGroup: 0,
							AlgorithmId:    bcn.Segment.ASEntries[0].Extensions.Irec.AlgorithmId,
						},
						peers)
				} else {
					// Otherwise, default values.
					err = p.Extender.Extend(ctx, bcn.Segment, bcn.InIfID,
						intf.TopoInfo().ID, false, nil, peers)
				}
				if err != nil {
					log.Error("Extending failed", "err", err)
					return
				}

				// Propagate to ingress gateway
				senderCtx, cancel := context.WithTimeout(ctx, defaultNewSenderTimeout)
				defer cancel()
				// SenderFactory is of type PoolBeaconSenderFactory so we can use the same sender for multiple beacons.
				sender, err := p.SenderFactory.NewSender(
					senderCtx,
					intf.TopoInfo().IA,
					intf.TopoInfo().ID,
					net.UDPAddrFromAddrPort(intf.TopoInfo().InternalAddr),
				)
				if err != nil {
					log.Error("Creating sender failed", "err", err)
					return
				}
				defer sender.Close()
				if err := sender.Send(ctx, bcn.Segment); err != nil {
					log.Error("Sending beacon failed", "dstIA", intf.TopoInfo().IA,
						"dstId", intf.TopoInfo().ID, "dstNH", intf.TopoInfo().InternalAddr, "err",
						err)
					return
				}
				// Mark beacon as propagated in egress db with the real expiry time
				err = p.Store.UpdateExpiry(ctx, beaconHash, intf, time.Now().Add(time.Hour))
				if err != nil {
					log.Error("Beacon DB Propagation Mark failed", "err", err)
					return
				}

				if bcn.Segment.ASEntries[0].Extensions.Irec != nil {
					intf.Propagate(time.Now(), HashToString(bcn.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash))
				} else {
					intf.Propagate(time.Now(), "")
				}
				success = true
			}()
		}
	}
	failed := 0
	for range egressBeacons {
		if !<-successCh {
			failed++
		}
	}
	wg.Wait()
	log.Info("Beacon propagation done", "beacons", len(request.Beacon), "real", len(egressBeacons)-failed, "expected", len(egressBeacons))
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
