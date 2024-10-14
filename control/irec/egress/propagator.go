package egress

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/scionproto/scion/control/irec/ingress"
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
	RacHandler            ingress.RacJobHandler
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

const defaultNewSenderTimeout = 30 * time.Second

func (p *Propagator) RequestPropagation(ctx context.Context, request *cppb.PropagationRequest) (*cppb.PropagationRequestResponse, error) {
	var wg sync.WaitGroup
	var err error
	ppT := procperf.GetNew(procperf.Propagated, fmt.Sprintf("%d", request.JobID))
	defer ppT.Write()
	timeParsingS := time.Now()
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
	timeParsingE := time.Now()
	ppT.AddDurationT(timeParsingS, timeParsingE) // 0
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
			pp := procperf.GetNew(procperf.Written, writer.WriterType().String())
			pp.SetNumBeacons(uint32(len(beaconIndexes)))
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
				return
			}
			timeWriterE := time.Now()

			if stats.Count > 0 {
				pp.AddDurationT(timeWriterS, timeWriterE)
				pp.Write()
			}
		}()
	}
	timeFilterS := time.Now()
	var egressBeacons []storage.EgressBeacon
	currIdx := -1
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
	timeFilterE := time.Now()
	//log.Info("RP; Beacon Half filtering", "beacons", totalNumber, "filtered", totalNumberFiltered, "time", time.Since(timeFilterS))
	ppT.AddDurationT(timeFilterS, timeFilterE) // 1
	ppT.SetNumBeacons(uint32(totalNumber))
	timeDBFilterS := time.Now()
	egressBeacons, err = p.Store.BeaconsThatShouldBePropagated(ctx, egressBeacons, time.Now().Add(3*defaultNewSenderTimeout))
	if err != nil {
		log.Error("Could not filter beacons to be propagated", "err", err)
		return &cppb.PropagationRequestResponse{}, err
	}
	timeDBFilterE := time.Now()
	ppT.AddDurationT(timeDBFilterS, timeDBFilterE) // 2
	totalNumberFiltered = 0
	for _, ebcn := range egressBeacons {
		totalNumberFiltered += len(ebcn.EgressIntfs)
	}
	//log.Info("RP; DB; Beacon filtering done", "beacons", totalNumber, "filtered", totalNumberFiltered, "time", timeFilterE.Sub(timeFilterS))
	go func() {
		defer log.HandlePanic()

		ctx := context.Background()

		senderByIntf := make(map[uint32]Sender)

		for _, ebcn := range egressBeacons {
			wg.Add(len(ebcn.EgressIntfs))
		}

		destIAs := make(map[addr.IA]struct{})
		succDestIAs := make(map[addr.IA]struct{})

		for _, ebcn := range egressBeacons {
			beaconHash := ebcn.BeaconHash
			for _, intfId := range ebcn.EgressIntfs {
				bcnId := procperf.GetFullId(beacons[ebcn.Index].Segment.GetLoggingID(), beacons[ebcn.Index].Segment.Info.SegmentID)
				pp := procperf.GetNew(procperf.PropagatedBcn, bcnId)
				pp.SetData(fmt.Sprintf("%d", request.JobID))
				intf := p.Interfaces[intfId]
				if intf.TopoInfo().ID != uint16(intfId) {
					log.Error("Interface ID mismatch", "intfId", intfId, "topoId", intf.TopoInfo().ID)
					continue
				}
				timeSenderS := time.Now()
				sender, ok := senderByIntf[intfId]
				if !ok {
					senderCtx, cancel := context.WithTimeout(context.Background(), defaultNewSenderTimeout)
					defer cancel()
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
					senderByIntf[intfId] = sender
					if _, ok := destIAs[intf.TopoInfo().IA]; !ok {
						destIAs[intf.TopoInfo().IA] = struct{}{}
					}
				}
				timeSenderE := time.Now()
				pp.AddDurationT(timeSenderS, timeSenderE) // 0
				beaconHash := beaconHash
				ebcn := ebcn
				go func() {
					defer log.HandlePanic()
					defer wg.Done()
					defer pp.Write()
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
					pp.AddDurationT(timeExtendS, timeExtendE) // 1
					nextId := procperf.GetFullId(segment.GetLoggingID(), segment.Info.SegmentID)
					pp.SetNextID(nextId)
					timeSendS := time.Now()
					if err := sender.Send(ctx, segment); err != nil {
						log.Error("Sending beacon failed", "dstIA", intf.TopoInfo().IA,
							"dstId", intf.TopoInfo().ID, "dstNH", intf.TopoInfo().InternalAddr, "err",
							err)
						return
					}
					timeSendE := time.Now()
					pp.AddDurationT(timeSendS, timeSendE) // 2
					timeUpdateS := time.Now()
					// Mark beacon as propagated in egress db with the real expiry time
					err = p.Store.UpdateExpiry(ctx, *beaconHash, intf, time.Now().Add(time.Hour))
					if err != nil {
						log.Error("Beacon DB Propagation Mark failed", "err", err)
						return
					}
					timeUpdateE := time.Now()
					pp.AddDurationT(timeUpdateS, timeUpdateE) // 3
					timeIntfPropagateS := time.Now()
					if segment.ASEntries[0].Extensions.Irec != nil {
						intf.Propagate(time.Now(), HashToString(segment.ASEntries[0].Extensions.Irec.AlgorithmHash))
					} else {
						intf.Propagate(time.Now(), "")
					}
					if _, ok := succDestIAs[intf.TopoInfo().IA]; !ok {
						succDestIAs[intf.TopoInfo().IA] = struct{}{}
					}
					timeIntfPropagateE := time.Now()
					pp.AddDurationT(timeIntfPropagateS, timeIntfPropagateE) // 4
					//log.Info("DB; Expiry updated", "time", timeUpdateE.Sub(timeUpdateS))
					//log.Info("BCN; Beacon sent", "extend", timeExtendE.Sub(timeExtendS), "sender", timeSenderE.Sub(timeSenderS), "send", timeSendE.Sub(timeSendS), "update expiry", timeUpdateE.Sub(timeUpdateS), "intf", timeIntfPropagateE.Sub(timeIntfPropagateS))
				}()
			}
		}
		wg.Wait()
		for _, sender := range senderByIntf {
			sender.Close()
		}

		for destIA := range destIAs {
			if _, ok := succDestIAs[destIA]; !ok {
				start := beacons[0].Segment.FirstIA()
				var intfGroup uint16
				algorithmHash := []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9} // Fallback RAC.
				var algorithmId uint32
				if beacons[0].Segment.ASEntries[0].Extensions.Irec != nil {
					intfGroup = beacons[0].Segment.ASEntries[0].Extensions.Irec.InterfaceGroup
					algorithmHash = beacons[0].Segment.ASEntries[0].Extensions.Irec.AlgorithmHash
					algorithmId = beacons[0].Segment.ASEntries[0].Extensions.Irec.AlgorithmId
				}
				p.RacHandler.MakeRacJobValid(ctx, &beacon.RacJobAttr{
					IsdAs:     start,
					IntfGroup: intfGroup,
					AlgHash:   algorithmHash,
					AlgId:     algorithmId,
				})
			}
		}
	}()

	// print db size
	// dbSize, err := p.Store.GetDBSize(ctx)
	// if err != nil {
	// 	log.Error("Could not get db size", "err", err)
	// } else {
	// 	log.Info("DB; Size", "size", dbSize)
	// }
	// timeWaitS := time.Now()
	// wg.Wait()
	// timeWaitE := time.Now()
	// ppT.AddDurationT(timeWaitS, timeWaitE) // 3
	// log.Info("RP; Beacon propagation waiting done", "time", timeWaitE.Sub(timeWaitS))
	// log.Info("RP; Beacon propagation done", "beacons", totalNumber, "filtered", totalNumberFiltered)
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
