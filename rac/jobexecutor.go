package rac

import (
	"context"
	"fmt"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/snet"
	"github.com/scionproto/scion/private/procperf"
	"github.com/scionproto/scion/rac/env"
	"time"

	"github.com/scionproto/scion/private/periodic"
)

var _ periodic.Task = (*JobExecutor)(nil)

type JobExecutor struct {
	Dialer           libgrpc.TCPDialer
	AlgCache         AlgorithmCache
	Environment      env.Environment
	CandidateSetSize uint32
	Mode             string
	exec             executor

	// Tick is mutable.
	Tick periodic.Tick
}

func (j *JobExecutor) Name() string {
	return "rac_job_executor"
}

func (j *JobExecutor) Run(ctx context.Context) {
	j.Tick.SetNow(time.Now())
	if err := j.exec.run(ctx); err != nil {
		log.FromCtx(ctx).Error("Error running job executor", "err", err)
	}
	j.Tick.UpdateLast()
}

func (j *JobExecutor) SetType(static bool) {
	if static {
		j.exec = &StaticJobExecutor{j}
	} else {
		j.exec = &DynamicJobExecutor{j}
		j.AlgCache = AlgorithmCache{Algorithms: make(map[string][]byte)}
	}
}

type executor interface {
	run(ctx context.Context) error
}

type DynamicJobExecutor struct {
	jobExec *JobExecutor
}

func (d *DynamicJobExecutor) run(ctx context.Context) error {
	conn, err := d.jobExec.Dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 100)
	if err != nil {
		return err
	}
	defer conn.Close()
	timeGrpcIngress1S := time.Now() // 0
	client := cppb.NewIngressIntraServiceClient(conn)
	// First get possible sources from the ingress gateway (source=originas, algorithmid, alghash combo)
	exec, err := client.GetJob(ctx, &cppb.RACBeaconRequest{IgnoreIntfGroup: false, Maximum: d.jobExec.CandidateSetSize}, libgrpc.RetryProfile...)
	if err != nil {
		return serrors.WrapStr("Error when retrieving beacon job", err)
	}
	timeGrpcIngress1E := time.Now() // 1
	//log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
	if exec.BeaconCount == 0 {
		return nil
	}
	bcnIds := make([]string, 0)
	for _, beacon := range exec.BeaconsUnopt {
		ps, err := seg.BeaconFromPB(beacon.PathSeg)
		if err != nil {
			return serrors.WrapStr("Error when converting path segment", err)
		}
		bcnIds = append(bcnIds, procperf.GetFullId(ps.GetLoggingID(), ps.Info.SegmentID))
	}
	for _, bcnId := range bcnIds {
		if err := procperf.AddTimestampsDoneBeacon(bcnId, procperf.Received, []time.Time{}, fmt.Sprintf("%d", exec.JobID)); err != nil {
			log.Error("PROCPERF: Error when receiving beacon", "err", err)
		}
	}
	// If there are PCB sources to process, get the job. This will mark the PCB's as taken such that other
	// RACS do not reprocess them.
	algorithm, _ := d.jobExec.AlgCache.Algorithms[string(exec.AlgorithmHash)]
	timeAlgorithmRetS := time.Now() // 2
	if d.jobExec.Mode != "native" {
		algResponse, err := client.GetAlgorithm(context.Background(), &cppb.AlgorithmRequest{AlgorithmHash: exec.AlgorithmHash})
		if err != nil {
			return serrors.WrapStr("Error when retrieving algorithm", err)
		}
		algorithm = algResponse.Code
		d.jobExec.AlgCache.Algorithms[string(exec.AlgorithmHash)] = algResponse.Code

	}
	timeAlgorithmRetE := time.Now() // 3
	res, err := d.jobExec.Environment.ExecuteDynamic(ctx, exec, algorithm, 0)
	if err != nil {
		return serrors.WrapStr("Error when executing rac for sources", err)
	}
	timeGrpcIngress2S := time.Now() // 4
	_, err = client.JobComplete(ctx, res)
	if err != nil {
		return serrors.WrapStr("Error when completing job", err)
	}
	timeGrpcIngress2E := time.Now() // 5
	//log.Info("GetJob time", "duration", timeGrpcIngress1E.Sub(timeGrpcIngress1S).Seconds())
	//log.Info("Execute time", "duration", timeGrpcIngress2S.Sub(timeAlgorithmRetE).Seconds())
	//log.Info("JobComplete time", "duration", timeGrpcIngress2E.Sub(timeGrpcIngress2S).Seconds())
	if err := procperf.AddTimestampsDoneBeacon(fmt.Sprintf("%d", exec.JobID), procperf.Processed, []time.Time{timeGrpcIngress1S, timeGrpcIngress1E, timeAlgorithmRetS, timeAlgorithmRetE, timeGrpcIngress2S, timeGrpcIngress2E}); err != nil {
		log.Error("PROCPERF: Error when processing job", "err", err)
	}
	return nil
}

type StaticJobExecutor struct {
	jobExec *JobExecutor
}

func (s *StaticJobExecutor) run(ctx context.Context) error {
	conn, err := s.jobExec.Dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 50)
	if err != nil {
		return err
	}
	defer conn.Close()

	timeGrpcIngress1S := time.Now() // 0
	client := cppb.NewIngressIntraServiceClient(conn)
	exec, err2 := client.GetBeacons(ctx, &cppb.BeaconQuery{Maximum: s.jobExec.CandidateSetSize})
	if err2 != nil {
		return serrors.WrapStr("Error when retrieving job for sources", err2)
	}
	timeGrpcIngress1E := time.Now() // 1
	bcnIds := make([]string, 0)
	for _, beacon := range exec.BeaconsUnopt {
		ps, err := seg.BeaconFromPB(beacon.PathSeg)
		if err != nil {
			return serrors.WrapStr("Error when converting path segment", err)
		}
		bcnIds = append(bcnIds, procperf.GetFullId(ps.GetLoggingID(), ps.Info.SegmentID))
	}
	for _, bcnId := range bcnIds {
		if err := procperf.AddTimestampsDoneBeacon(bcnId, procperf.Received, []time.Time{}, fmt.Sprintf("%d", exec.JobID)); err != nil {
			log.Error("PROCPERF: Error when receiving beacon", "err", err)
		}
	}
	log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
	timeExecS := time.Now() // 2
	res, err := s.jobExec.Environment.ExecuteStatic(ctx, exec, 0)
	if err != nil {
		return serrors.WrapStr("Error when executing rac for sources", err)
	}
	timeGrpcIngress2S := time.Now() // 3
	_, err = client.JobComplete(ctx, res)
	if err != nil {
		return serrors.WrapStr("Error when completing job", err)
	}
	timeGrpcIngress2E := time.Now() // 4
	if err := procperf.AddTimestampsDoneBeacon(fmt.Sprintf("%d", exec.JobID), procperf.Processed, []time.Time{timeGrpcIngress1S, timeGrpcIngress1E, timeExecS, timeGrpcIngress2S, timeGrpcIngress2E}); err != nil {
		log.Error("PROCPERF: Error when processing job", "err", err)
	}
	return nil
}
