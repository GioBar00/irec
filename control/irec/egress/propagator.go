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

	var egressBeacons []storage.EgressBeacon
	currIdx := -1
	timeFilterS := time.Now()
	totalNumber := 0
	totalNumberFiltered := 0
	for _, i := range beaconIndexes {
		beaconHash := HashBeacon(beacons[i].Segment)
		for _, intfId := range request.Beacon[i].EgressIntfs {
			totalNumber++
			intf := p.Interfaces[intfId]
			if intf == nil {
				log.Error("Attempt to send beacon on non-existent interface", "egress_interface", intfId)
				continue
			}
			if !p.PropagationFilter(intf) {
				log.Error("Attempt to send beacon on filtered egress interface", "egress_interface", intfId)
				continue
			}
			if p.shouldIgnore(beacons[i].Segment, intf) {
				continue
			}
			if currIdx >= 0 && egressBeacons[currIdx].Index == i {
				egressBeacons[currIdx].EgressIntfs = append(egressBeacons[currIdx].EgressIntfs, intfId)
			} else {
				egressBeacons = append(egressBeacons, storage.EgressBeacon{
					BeaconHash:  &beaconHash,
					EgressIntfs: []uint32{intfId},
					Index:       i,
				})
				currIdx++
			}
			totalNumberFiltered++
		}
	}
	log.Info("RP; Beacon Half filtering", "beacons", totalNumber, "filtered", totalNumberFiltered, "time", time.Since(timeFilterS))
	timeFilterS = time.Now()
	egressBeacons, err = p.Store.BeaconsThatShouldBePropagated(ctx, egressBeacons, time.Now().Add(3*defaultNewSenderTimeout))
	if err != nil {
		log.Error("Could not filter beacons to be propagated", "err", err)
		return &cppb.PropagationRequestResponse{}, err
	}
	timeFilterE := time.Now()
	totalNumberFiltered = 0
	for _, ebcn := range egressBeacons {
		totalNumberFiltered += len(ebcn.EgressIntfs)
	}
	log.Info("RP; DB; Beacon filtering done", "beacons", totalNumber, "filtered", totalNumberFiltered, "time", timeFilterE.Sub(timeFilterS))
	senderByIntf := make(map[uint32]Sender)
	senderCtx, cancel := context.WithTimeout(ctx, defaultNewSenderTimeout)
	defer cancel()
	for _, ebcn := range egressBeacons {
		//bcn := beacons[ebcn.Index]
		beaconHash := ebcn.BeaconHash
		for _, intfId := range ebcn.EgressIntfs {
			intf := p.Interfaces[intfId]
			timeSenderS := time.Now()
			sender, ok := senderByIntf[intfId]
			if !ok {
				sender, err = p.SenderFactory.NewSender(
					senderCtx,
					intf.TopoInfo().IA,
					intf.TopoInfo().ID,
					net.UDPAddrFromAddrPort(intf.TopoInfo().InternalAddr),
				)
				if err != nil {
					log.Error("Creating sender failed", "err", err)
					continue
				}
				defer sender.Close()
				senderByIntf[intfId] = sender
			}
			wg.Add(1)
			timeSenderE := time.Now()
			//bcn := bcn
			beaconHash := beaconHash
			ebcn := ebcn
			go func() {
				defer log.HandlePanic()
				defer wg.Done()
				segment, err := seg.BeaconFromPB(request.Beacon[ebcn.Index].PathSeg)
				if err != nil {
					log.Error("Could not parse beacon segment", "err", err)
					return
				}
				timeExtendS := time.Now()
				// If the Origin-AS used Irec, we copy the algorithmID and hash from the first as entry
				peers := SortedIntfs(p.AllInterfaces, topology.Peer)
				if segment.ASEntries[0].Extensions.Irec != nil {
					err = p.Extender.Extend(ctx, segment, beacons[ebcn.Index].InIfID,
						intf.TopoInfo().ID, true,
						&irec.Irec{
							AlgorithmHash:  segment.ASEntries[0].Extensions.Irec.AlgorithmHash,
							InterfaceGroup: 0,
							AlgorithmId:    segment.ASEntries[0].Extensions.Irec.AlgorithmId,
						},
						peers)
				} else {
					// Otherwise, default values.
					err = p.Extender.Extend(ctx, segment, beacons[ebcn.Index].InIfID,
						intf.TopoInfo().ID, false, nil, peers)
				}
				if err != nil {
					log.Error("Extending failed", "err", err)
					return
				}
				timeExtendE := time.Now()
				// timeSenderS := time.Now()
				// // Propagate to ingress gateway
				// senderCtx, cancel := context.WithTimeout(ctx, defaultNewSenderTimeout)
				// defer cancel()
				// // SenderFactory is of type PoolBeaconSenderFactory so we can use the same sender for multiple beacons.
				// sender, err := p.SenderFactory.NewSender(
				// 	senderCtx,
				// 	intf.TopoInfo().IA,
				// 	intf.TopoInfo().ID,
				// 	net.UDPAddrFromAddrPort(intf.TopoInfo().InternalAddr),
				// )
				// if err != nil {
				// 	log.Error("Creating sender failed", "err", err)
				// 	return
				// }
				// defer sender.Close()
				// timeSenderE := time.Now()
				timeSendS := time.Now()
				if err := sender.Send(ctx, segment); err != nil {
					log.Error("Sending beacon failed", "dstIA", intf.TopoInfo().IA,
						"dstId", intf.TopoInfo().ID, "dstNH", intf.TopoInfo().InternalAddr, "err",
						err)
					return
				}
				timeSendE := time.Now()
				timeUpdateS := time.Now()
				// Mark beacon as propagated in egress db with the real expiry time
				err = p.Store.UpdateExpiry(ctx, *beaconHash, intf, time.Now().Add(time.Hour))
				if err != nil {
					log.Error("Beacon DB Propagation Mark failed", "err", err)
					return
				}
				timeUpdateE := time.Now()
				timeIntfPropagateS := time.Now()
				if segment.ASEntries[0].Extensions.Irec != nil {
					intf.Propagate(time.Now(), HashToString(segment.ASEntries[0].Extensions.Irec.AlgorithmHash))
				} else {
					intf.Propagate(time.Now(), "")
				}
				timeIntfPropagateE := time.Now()
				log.Info("DB; Expiry updated", "time", timeUpdateE.Sub(timeUpdateS))
				log.Info("BCN; Beacon sent", "extend", timeExtendE.Sub(timeExtendS), "sender", timeSenderE.Sub(timeSenderS), "send", timeSendE.Sub(timeSendS), "update expiry", timeUpdateE.Sub(timeUpdateS), "intf", timeIntfPropagateE.Sub(timeIntfPropagateS))
			}()
		}
	}
	//log.Info("RP; Waiting for propagation results")
	timeWaitS := time.Now()
	wg.Wait()
	timeWaitE := time.Now()
	log.Info("RP; Beacon propagation waiting done", "time", timeWaitE.Sub(timeWaitS))
	log.Info("RP; Beacon propagation done", "beacons", totalNumber, "filtered", totalNumberFiltered)
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
