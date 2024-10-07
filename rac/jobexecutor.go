package rac

import (
	"context"
	"fmt"
	"time"

	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"github.com/scionproto/scion/pkg/snet"
	"github.com/scionproto/scion/private/procperf"
	"github.com/scionproto/scion/rac/env"

	"github.com/scionproto/scion/private/periodic"
)

var _ periodic.Task = (*JobExecutor)(nil)

type JobExecutor struct {
	Dialer           libgrpc.TCPDialer
	AlgCache         AlgorithmCache
	Environment      env.Environment
	CandidateSetSize uint32
	Mode             string
	JobRunner

	// Tick is mutable.
	Tick periodic.Tick
}

func (j *JobExecutor) Name() string {
	return "rac_job_executor"
}

func (j *JobExecutor) Run(ctx context.Context) {
	j.Tick.SetNow(time.Now())
	if err := j.run(ctx, j); err != nil {
		log.FromCtx(ctx).Error("Error running job executor", "err", err)
	}
	j.Tick.UpdateLast()
}

func (j *JobExecutor) Task(static bool) periodic.Task {
	if static {
		j.JobRunner = &StaticJobRunner{}
	} else {
		j.AlgCache = AlgorithmCache{Algorithms: make(map[string][]byte)}
		j.JobRunner = &DynamicJobRunner{}
	}
	return j
}

type JobRunner interface {
	run(ctx context.Context, j *JobExecutor) error
}

type DynamicJobRunner struct {
}

func (d *DynamicJobRunner) run(ctx context.Context, j *JobExecutor) error {
	pp := procperf.GetNew(procperf.Processed, "")
	timeConnS := time.Now()
	conn, err := j.Dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 100)
	if err != nil {
		return err
	}
	defer conn.Close()
	timeConnE := time.Now()
	pp.AddDurationT(timeConnS, timeConnE) // 0
	timeGrpcIngress1S := time.Now()
	client := cppb.NewIngressIntraServiceClient(conn)
	// First get possible sources from the ingress gateway (source=originas, algorithmid, alghash combo)
	exec, err := client.GetJob(ctx, &cppb.RACBeaconRequest{IgnoreIntfGroup: false, Maximum: j.CandidateSetSize}, libgrpc.RetryProfile...)
	if err != nil {
		return serrors.WrapStr("Error when retrieving beacon job", err)
	}
	timeGrpcIngress1E := time.Now()
	pp.SetID(fmt.Sprintf("%d", exec.JobID))
	pp.AddDurationT(timeGrpcIngress1S, timeGrpcIngress1E) // 1
	defer pp.Write()
	//log.Info("GetJob time", "duration", timeGrpcIngress1E.Sub(timeGrpcIngress1S).Seconds())
	//log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
	if exec.BeaconCount == 0 {
		return nil
	}
	// If there are PCB sources to process, get the job. This will mark the PCB's as taken such that other
	// RACS do not reprocess them.
	algorithm := j.AlgCache.Algorithms[string(exec.AlgorithmHash)]
	timeAlgorithmRetS := time.Now()
	if j.Mode != "native" {
		algResponse, err := client.GetAlgorithm(context.Background(), &cppb.AlgorithmRequest{AlgorithmHash: exec.AlgorithmHash})
		if err != nil {
			return serrors.WrapStr("Error when retrieving algorithm", err)
		}
		algorithm = algResponse.Code
		j.AlgCache.Algorithms[string(exec.AlgorithmHash)] = algResponse.Code

	}
	timeAlgorithmRetE := time.Now()
	pp.AddDurationT(timeAlgorithmRetS, timeAlgorithmRetE) // 2
	timeExecutionS := time.Now()
	res, err := j.Environment.ExecuteDynamic(ctx, exec, algorithm, 0)
	if err != nil {
		return serrors.WrapStr("Error when executing rac for sources", err)
	}
	timeExecutionE := time.Now()
	pp.AddDurationT(timeExecutionS, timeExecutionE) // 3
	timeGrpcIngress2S := time.Now()
	_, err = client.JobComplete(ctx, res)
	if err != nil {
		return serrors.WrapStr("Error when completing job", err)
	}
	timeGrpcIngress2E := time.Now()
	pp.AddDurationT(timeGrpcIngress2S, timeGrpcIngress2E) // 4
	//log.Info("Execute time", "duration", timeGrpcIngress2S.Sub(timeAlgorithmRetE).Seconds())
	//log.Info("JobComplete time", "duration", timeGrpcIngress2E.Sub(timeGrpcIngress2S).Seconds())
	return nil
}

type StaticJobRunner struct{}

func (s *StaticJobRunner) run(ctx context.Context, j *JobExecutor) error {
	pp := procperf.GetNew(procperf.Processed, "")
	timeConnS := time.Now()
	conn, err := j.Dialer.DialLimit(ctx, &snet.SVCAddr{SVC: addr.SvcCS}, 50)
	if err != nil {
		return err
	}
	defer conn.Close()
	timeConnE := time.Now()
	pp.AddDurationT(timeConnS, timeConnE) // 0
	timeGrpcIngress1S := time.Now()
	client := cppb.NewIngressIntraServiceClient(conn)
	exec, err2 := client.GetBeacons(ctx, &cppb.BeaconQuery{Maximum: j.CandidateSetSize})
	if err2 != nil {
		return serrors.WrapStr("Error when retrieving job for sources", err2)
	}
	timeGrpcIngress1E := time.Now()
	pp.SetID(fmt.Sprintf("%d", exec.JobID))
	pp.AddDurationT(timeGrpcIngress1S, timeGrpcIngress1E) // 1
	defer pp.Write()
	//log.Info(fmt.Sprintf("Processing %d beacons.", len(exec.RowIds)))
	timeExecS := time.Now()
	res, err := j.Environment.ExecuteStatic(ctx, exec, 0)
	if err != nil {
		return serrors.WrapStr("Error when executing rac for sources", err)
	}
	timeExecE := time.Now()
	pp.AddDurationT(timeExecS, timeExecE) // 2
	timeGrpcIngress2S := time.Now()
	_, err = client.JobComplete(ctx, res)
	if err != nil {
		return serrors.WrapStr("Error when completing job", err)
	}
	timeGrpcIngress2E := time.Now()
	pp.AddDurationT(timeGrpcIngress2S, timeGrpcIngress2E) // 3
	return nil
}
