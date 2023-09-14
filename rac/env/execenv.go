package env

import (
	"context"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	"github.com/scionproto/scion/rac/config"
)

type Environment interface {
	Initialize()
	InitStaticAlgo(alg config.RACAlgorithm)
	ExecuteDynamic(ctx context.Context, job *cppb.RACJob, code []byte, counter int32) (*cppb.JobCompleteNotify, error)
	ExecuteStatic(ctx context.Context, job *cppb.RACJob, counter int32) (*cppb.JobCompleteNotify, error)
}
