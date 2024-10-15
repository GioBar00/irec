package ingress

import (
	"context"
	"github.com/scionproto/scion/control/beaconing"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/pkg/log"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/private/periodic"
	"github.com/scionproto/scion/private/procperf"
	"github.com/scionproto/scion/private/topology"
	"time"
)

var _ periodic.Task = (*WriteScheduler)(nil)

// WriteScheduler is used to periodically write path segments at the configured
// writer.
type WriteScheduler struct {
	// Provider is used to query for segments.
	Provider beaconing.SegmentProvider
	// Intfs gives access to the interfaces this CS beacons over.
	Intfs *ifstate.Interfaces
	// Type is the type of segments that should be queried from the Provider.
	Type seg.Type
	// Write is used to write the segments once the scheduling determines it is
	// time to write.
	Writer beaconing.Writer

	// Tick is mutable. It's used to determine when to call write.
	Tick periodic.Tick
	// lastWrite indicates the time of the last successful write.
	lastWrite time.Time
}

// Name returns the tasks name.
func (r *WriteScheduler) Name() string {
	// XXX(lukedirtwalker): this name is used in metrics and changing it would
	// be a breaking change.
	return "control_beaconing_registrar"
}

// Run writes path segments using the configured writer.
func (r *WriteScheduler) Run(ctx context.Context) {
	r.Tick.SetNow(time.Now())
	if err := r.run(ctx); err != nil {
		log.FromCtx(ctx).Error("Unable to register", "seg_type", r.Type, "err", err)
	}
	r.Tick.UpdateLast()
}

func (r *WriteScheduler) run(ctx context.Context) error {
	if !(r.Tick.Overdue(r.lastWrite) || r.Tick.Passed()) {
		return nil
	}
	pp := procperf.GetNew(procperf.Written, r.Type.String())
	timeGetS := time.Now()
	segments, err := r.Provider.SegmentsToRegister(ctx, r.Type)
	if err != nil {
		return err
	}
	timeGetE := time.Now()
	pp.SetNumBeacons(uint32(len(segments)))
	pp.AddDurationT(timeGetS, timeGetE)
	timeWriterS := time.Now()
	peers := beaconing.SortedIntfs(r.Intfs, topology.Peer)
	stats, err := r.Writer.Write(ctx, segments, peers, true)
	if err != nil {
		return err
	}
	timeWriterE := time.Now()
	pp.AddDurationT(timeWriterS, timeWriterE)
	r.logSummary(ctx, &beaconing.Summary{Count: stats.Count, Srcs: stats.StartIAs})
	if stats.Count > 0 {
		r.lastWrite = r.Tick.Now()
		pp.Write()
	}
	return err
}

func (r *WriteScheduler) logSummary(ctx context.Context, s *beaconing.Summary) {
	logger := log.FromCtx(ctx)
	if r.Tick.Passed() {
		logger.Debug("Registered beacons", "seg_type", r.Type, "count", s.Count,
			"start_isd_ases", len(s.Srcs))
		return
	}
	if s.Count > 0 {
		logger.Debug("Registered beacons after stale period",
			"seg_type", r.Type, "count", s.Count, "start_isd_ases", len(s.Srcs))
	}
}
