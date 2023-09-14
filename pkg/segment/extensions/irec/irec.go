package irec

import (
	"github.com/scionproto/scion/pkg/addr"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"time"
)

type Irec struct {
	AlgorithmHash        []byte
	AlgorithmId          uint32
	InterfaceGroup       uint16
	PullBased            bool
	PullBasedTarget      addr.IA
	PullBasedPeriod      time.Duration
	PullBasedHyperPeriod time.Duration
	PullBasedMinBeacons  uint32
}

func ExtensionFromPB(d *cppb.IRECExtension) *Irec {
	if d == nil {
		return nil
	}

	//if d.AlgorithmHash
	return &Irec{
		AlgorithmHash:        d.AlgorithmHash,
		AlgorithmId:          d.AlgorithmId,
		InterfaceGroup:       uint16(d.InterfaceGroup),
		PullBased:            d.Pullbased,
		PullBasedTarget:      addr.IA(d.PullbasedTarget),
		PullBasedMinBeacons:  d.PullbasedMinBeacons,
		PullBasedPeriod:      time.Millisecond * time.Duration(d.PullbasedPeriod),
		PullBasedHyperPeriod: time.Millisecond * time.Duration(d.PullbasedHyperPeriod),
	}
}

func ExtensionToPB(d *Irec) *cppb.IRECExtension {
	if d == nil {
		return nil
	}

	return &cppb.IRECExtension{
		AlgorithmHash:        d.AlgorithmHash,
		AlgorithmId:          d.AlgorithmId,
		InterfaceGroup:       uint32(d.InterfaceGroup),
		Pullbased:            d.PullBased,
		PullbasedTarget:      uint64(d.PullBasedTarget),
		PullbasedPeriod:      uint64(d.PullBasedPeriod.Milliseconds()),
		PullbasedHyperPeriod: uint64(d.PullBasedHyperPeriod.Milliseconds()),
		PullbasedMinBeacons:  d.PullBasedMinBeacons,
	}
}
