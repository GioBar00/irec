//go:build !timing

package native

import "C"
import (
	"context"
	"math"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/pkg/log"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/rac"
	"github.com/scionproto/scion/rac/config"
)

type NativeEnv struct {
	Writer rac.EgressWriter
	Static bool
}

func (e *NativeEnv) Initialize() {

}
func (e *NativeEnv) InitStaticAlgo(alg config.RACAlgorithm) {
}

func (e *NativeEnv) ExecuteDynamic(ctx context.Context, job *cppb.RACJob, code []byte, counter int32) (*cppb.JobCompleteNotify, error) {
	return e.selection(ctx, job)
}

func (e *NativeEnv) ExecuteStatic(ctx context.Context, job *cppb.RACJob, counter int32) (*cppb.JobCompleteNotify, error) {
	return e.selection(ctx, job)
}
func (e *NativeEnv) selectMostDiverse(beacons []*cppb.IRECBeaconUnopt, start int, end int, best int) (int, int) {
	if len(beacons) == 0 || end > len(beacons) {
		return 0, -1
	}

	maxDiversity := -1
	minLen := math.MaxUint16
	var diverse int

	pseg, err := seg.SegmentFromPB(beacons[best].PathSeg)
	bestbcn := beacon.Beacon{Segment: pseg}
	if err != nil {
		return 0, -1
	}
	for i := start; i < end; i++ {
		pseg, err := seg.SegmentFromPB(beacons[i].PathSeg)
		bcn := beacon.Beacon{Segment: pseg}
		if err != nil {
			return 0, -1
		}
		diversity := bestbcn.Diversity(bcn)
		l := len(bcn.Segment.ASEntries)

		if diversity > maxDiversity || (diversity == maxDiversity && minLen > l) {
			diverse, minLen, maxDiversity = i, l, diversity
		}
	}
	return diverse, maxDiversity
}
func (e *NativeEnv) selection(ctx context.Context, job *cppb.RACJob) (*cppb.JobCompleteNotify, error) {
	//log.Info("starting selectione")
	selection := make([]*cppb.BeaconAndEgressIntf, 0)
	selectedBeacons := make([]*cppb.EgressBeacon, 0)
	resultSize := 20
	if len(job.BeaconsUnopt) <= resultSize {
		for _, i := range job.BeaconsUnopt {
			selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
				PathSeg:         i.PathSeg,
				InIfId:          i.InIfId,
				EgressIntfs:     job.PropIntfs,
				PullbasedTarget: 0,
			})
		}

	} else {
		for i := 0; i < resultSize-1; i++ {
			selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
				PathSeg:         job.BeaconsUnopt[i].PathSeg,
				InIfId:          job.BeaconsUnopt[i].InIfId,
				EgressIntfs:     job.PropIntfs,
				PullbasedTarget: 0,
			})
		}

		_, diversity := e.selectMostDiverse(job.BeaconsUnopt, 0, resultSize-1, 0)
		// Check if we find a more diverse beacon in the rest.
		mostDiverseRest, diversityRest := e.selectMostDiverse(job.BeaconsUnopt, 19, len(job.BeaconsUnopt), 0)
		if diversityRest > diversity {

			selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
				PathSeg:         job.BeaconsUnopt[mostDiverseRest].PathSeg,
				InIfId:          job.BeaconsUnopt[mostDiverseRest].InIfId,
				EgressIntfs:     job.PropIntfs,
				PullbasedTarget: 0,
			})
		} else {
			// If the most diverse beacon was already served, serve shortest from the
			// rest.

			selectedBeacons = append(selectedBeacons, &cppb.EgressBeacon{
				PathSeg:         job.BeaconsUnopt[resultSize-1].PathSeg,
				InIfId:          job.BeaconsUnopt[resultSize-1].InIfId,
				EgressIntfs:     job.PropIntfs,
				PullbasedTarget: 0,
			})
		}
	}
	err := e.Writer.WriteBeacons(ctx, selectedBeacons)
	if err != nil {
		log.Info("err", "msg", err)
		//	return &racpb.ExecutionResponse{}, selection, err
	}
	// selectMostDiverse selects the most diverse beacon compared to the provided best beacon from all
	// provided beacons and returns it and its diversity.
	if e.Static {
		return &cppb.JobCompleteNotify{
			RowIDs:    []int64{},
			Selection: selection,
		}, nil
	}
	return &cppb.JobCompleteNotify{
		RowIDs:    job.RowIds,
		Selection: selection,
	}, nil
}
