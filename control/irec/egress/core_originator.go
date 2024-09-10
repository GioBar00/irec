package egress

import (
	"context"
	"crypto/rand"
	"math/big"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scionproto/scion/private/procperf"

	beaconing "github.com/scionproto/scion/control/beaconing"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/segment/extensions/irec"
)

// The Core Originator originates for each algorithm and intfgroup.
type OriginationAlgorithm struct {
	ID   uint32
	Hash []byte
}

// Originator originates beacons. It should only be used by core ASes.
type BasicOriginator struct {
	Extender              Extender
	SenderFactory         SenderFactory
	IA                    addr.IA
	Intfs                 []*ifstate.Interface
	OriginatePerIntfGroup bool
}

// The originator which requires PCBs to be processed by a specific algorithm, this is periodically executed.
type AlgorithmOriginator struct {
	*BasicOriginator
	OriginationAlgorithms []OriginationAlgorithm

	// Tick is mutable.
	Tick beaconing.Tick
}

// The on-demand originator for pull-based beacons, executed on a request given through the SCION Daemon.
type PullBasedOriginator struct {
	*BasicOriginator
}

// Name returns the tasks name.
func (o *AlgorithmOriginator) Name() string {
	return "control_beaconing_originator"
}

// Run originates core and downstream beacons.
func (o *AlgorithmOriginator) Run(ctx context.Context) {
	o.Tick.SetNow(time.Now())
	o.originateBeacons(ctx)
	o.Tick.UpdateLast()
}

// Responsible for orginating beacons for each of the IREC algorithm hashes that are configured for this AS.
func (o *AlgorithmOriginator) originateBeacons(ctx context.Context) {
	//intfs := o.Intfs
	intfs, groupsPerIntf := o.needBeacon(o.Intfs)
	sort.Slice(intfs, func(i, j int) bool {
		return intfs[i].TopoInfo().ID < intfs[j].TopoInfo().ID
	})
	sort.Slice(groupsPerIntf, func(i, j int) bool {
		return intfs[i].TopoInfo().ID < intfs[j].TopoInfo().ID
	})
	if len(intfs) == 0 || len(o.OriginationAlgorithms) == 0 {
		return
	}
	// Only log on info and error level every propagation period to reduce
	// noise. The offending logs events are redirected to debug level.
	silent := !o.Tick.Passed()
	logger := beaconing.WithSilent(ctx, silent)

	numOrigBeacons := 0
	for i, intf := range intfs {
		if groupsPerIntf[i] != nil {
			numOrigBeacons += len(groupsPerIntf[i])
		} else if len(intf.TopoInfo().Groups) > 0 {
			numOrigBeacons += len(intf.TopoInfo().Groups)
		} else {
			numOrigBeacons++
		}
	}

	// Create atomic counter for number of beacons originated.
	var numBeacons atomic.Uint32

	var wg sync.WaitGroup
	wg.Add(len(intfs) * len(o.OriginationAlgorithms))
	for _, alg := range o.OriginationAlgorithms {
		for i, intf := range intfs {
			b := intfOriginator{
				BasicOriginator: o.BasicOriginator,
				intf:            intf,
				timestamp:       time.Now(),
				algId:           alg.ID,
				algHash:         alg.Hash,
				pullbased:       false,
			}
			i := i
			intf := intf
			go func() {
				defer log.HandlePanic()
				defer wg.Done()

				if err := b.originateMessage(ctx, groupsPerIntf[i]); err != nil {
					logger.Info("Unable to originate on interface",
						"egress_interface", b.intf.TopoInfo().ID, "err", err)
				} else {
					if groupsPerIntf[i] != nil {
						numBeacons.Add(uint32(len(groupsPerIntf[i])))
					} else if len(intf.TopoInfo().Groups) > 0 {
						numBeacons.Add(uint32(len(intf.TopoInfo().Groups)))
					} else {
						numBeacons.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()
	logger.Debug("Originated beacons", "Real", numBeacons.Load(), "Expected", numOrigBeacons)
}

// needBeacon returns a list of interfaces that need a beacon.
func (o *AlgorithmOriginator) needBeacon(active []*ifstate.Interface) ([]*ifstate.Interface, [][]uint16) {
	var groupsPerIntf [][]uint16
	if o.Tick.Passed() {
		for i := 0; i < len(active); i++ {
			groupsPerIntf = append(groupsPerIntf, nil)
		}
		return active, groupsPerIntf
	}
	var stale []*ifstate.Interface
	for _, intf := range active {
		topoInfo := intf.TopoInfo()
		var groups []uint16
		if len(topoInfo.Groups) == 0 {
			groups = []uint16{0} // Default to sending to all groups.
		} else {
			groups = topoInfo.Groups
		}
		addedIntf := false
		for _, intfGroup := range groups {
			if o.Tick.Overdue(intf.LastOriginate(intfGroup)) {
				if !addedIntf {
					stale = append(stale, intf)
					groupsPerIntf = append(groupsPerIntf, []uint16{intfGroup})
					addedIntf = true
				} else {
					groupsPerIntf[len(groupsPerIntf)-1] = append(groupsPerIntf[len(groupsPerIntf)-1], intfGroup)
				}
			}
		}
	}
	return stale, groupsPerIntf
}

// Responsible for originating a pull-based beacon
func (o *PullBasedOriginator) OriginatePullBasedBeacon(ctx context.Context, alg OriginationAlgorithm, targetIA addr.IA, period, hyperperiod time.Duration, minBeacons uint32) {
	intfs := o.Intfs
	sort.Slice(intfs, func(i, j int) bool {
		return intfs[i].TopoInfo().ID < intfs[j].TopoInfo().ID
	})
	if len(intfs) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(intfs))
	for _, intf := range intfs {
		b := intfOriginator{
			BasicOriginator:      o.BasicOriginator,
			intf:                 intf,
			timestamp:            time.Now(),
			algId:                alg.ID,
			algHash:              alg.Hash,
			pullbased:            true,
			pullbasedTarget:      targetIA,
			pullBasedPeriod:      period,
			pullBasedHyperPeriod: hyperperiod,
			pullBasedMinBeacons:  minBeacons,
		}
		go func() {
			defer log.HandlePanic()
			defer wg.Done()

			if err := b.originateMessage(ctx, nil); err != nil {
				log.Info("Unable to originate pullbased beacon on interface",
					"egress_interface", b.intf.TopoInfo().ID, "err", err)
			}
		}()
	}

	wg.Wait()
}

// intfOriginator originates one beacon on the given interface.
type intfOriginator struct {
	*BasicOriginator
	intf                 *ifstate.Interface
	timestamp            time.Time
	algId                uint32
	algHash              []byte
	pullbased            bool
	pullbasedTarget      addr.IA
	pullBasedMinBeacons  uint32
	pullBasedHyperPeriod time.Duration
	pullBasedPeriod      time.Duration
}

// originateBeacon originates a beacon on the given ifid.
func (o *intfOriginator) originateMessage(ctx context.Context, groups []uint16) error {
	topoInfo := o.intf.TopoInfo()

	senderStart := time.Now()
	duration := time.Duration(5 * max(len(topoInfo.Groups), 1))
	senderCtx, cancelF := context.WithTimeout(ctx, duration*time.Second)

	defer cancelF()

	sender, err := o.SenderFactory.NewSender(
		senderCtx,
		topoInfo.IA,
		topoInfo.ID,
		net.UDPAddrFromAddrPort(topoInfo.InternalAddr),
	)
	if err != nil {
		return serrors.WrapStr("getting beacon sender", err,
			"waited_for", time.Since(senderStart).String())
	}
	defer sender.Close()

	if groups == nil {
		if len(topoInfo.Groups) == 0 {
			groups = []uint16{0} // Default to sending to all groups.
		} else {
			groups = topoInfo.Groups
		}
	}

	if o.OriginatePerIntfGroup {
		for _, intfGroup := range groups {
			sendStart := time.Now()
			beacon, err := o.createBeacon(ctx, intfGroup)
			if o.pullbased {
				log.Debug("Originating a pullbased beacon", "target", o.pullbasedTarget, "intfGroup", intfGroup)
			}
			if err != nil {
				return serrors.WrapStr("creating beacon", err)
			}
			idCompStart := time.Now()
			bcnId := procperf.GetFullId(beacon.GetLoggingID(), beacon.Info.SegmentID)
			idCompStop := time.Now()
			if err := sender.Send(ctx, beacon); err != nil {
				return serrors.WrapStr("sending beacon", err,
					"waited_for", time.Since(sendStart).String(),
				)
			}
			t := time.Now().Add(-idCompStop.Sub(idCompStart))
			if err := procperf.AddTimeDoneBeacon(bcnId, procperf.Originated, sendStart, t, bcnId); err != nil {
				return serrors.WrapStr("PROCPERF: error done beacon", err)
			}
			o.intf.Originate(time.Now(), intfGroup)
		}
	} else {
		sendStart := time.Now()
		beacon, err := o.createBeacon(ctx, 0)
		if o.pullbased {
			log.Debug("Originating a pullbased beacon", "target", o.pullbasedTarget)
		}
		if err != nil {
			return serrors.WrapStr("creating beacon", err)
		}
		idCompStart := time.Now()
		bcnId := procperf.GetFullId(beacon.GetLoggingID(), beacon.Info.SegmentID)
		idCompElapsed := time.Since(idCompStart)
		if err := sender.Send(ctx, beacon); err != nil {
			return serrors.WrapStr("sending beacon", err,
				"waited_for", time.Since(sendStart).String(),
			)
		}
		t := time.Now().Add(-idCompElapsed)
		if err := procperf.AddTimeDoneBeacon(bcnId, procperf.Originated, sendStart, t, bcnId); err != nil {
			return serrors.WrapStr("PROCPERF: error done beacon", err)
		}
		o.intf.Originate(time.Now(), 0)
	}
	return nil
}
func (o *intfOriginator) createBeacon(ctx context.Context, intfGroup uint16) (*seg.PathSegment, error) {
	segID, err := rand.Int(rand.Reader, big.NewInt(1<<16))
	if err != nil {
		return nil, err
	}
	beacon, err := seg.CreateSegment(o.timestamp, uint16(segID.Uint64()))
	if err != nil {
		return nil, serrors.WrapStr("creating segment", err)
	}
	irecExt := &irec.Irec{
		AlgorithmHash:  o.algHash,
		InterfaceGroup: intfGroup,
		AlgorithmId:    o.algId,
	}
	if o.pullbased {
		irecExt.PullBased = true
		irecExt.PullBasedTarget = o.pullbasedTarget
		irecExt.PullBasedMinBeacons = o.pullBasedMinBeacons
		irecExt.PullBasedPeriod = o.pullBasedPeriod
		irecExt.PullBasedHyperPeriod = o.pullBasedHyperPeriod
	}
	if err := o.Extender.Extend(ctx, beacon, 0, o.intf.TopoInfo().ID, true, irecExt, nil); err != nil {
		return nil, serrors.WrapStr("extending segment", err)
	}
	return beacon, nil
}
