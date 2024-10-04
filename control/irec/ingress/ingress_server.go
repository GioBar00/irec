package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/config"
	"github.com/scionproto/scion/control/irec/ingress/storage"
	"github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/common"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/slayers/path/scion"
	"github.com/scionproto/scion/pkg/snet"
)

const (
	defaultAgeingFactor    = 1.0
	defaultPullBasedFactor = 1.0
	defaultGroupSizeFactor = 1.0
)

type IncomingBeaconHandler interface {
	HandleBeacon(ctx context.Context, b beacon.Beacon, peer *snet.UDPAddr) error
}

type IngressServer struct {
	IncomingHandler       IncomingBeaconHandler
	IngressDB             storage.IngressStore
	PropagationInterfaces []uint32
	Dialer                *grpc.TCPDialer
}

func (i *IngressServer) LoadAlgorithms(ctx context.Context, algs []config.AlgorithmInfo) {
	hasFallback := false
	for _, alg := range algs {
		file := alg.File
		log.Info("Attempting to load algorithm, " + file)
		fileBinary, err := os.ReadFile(file)
		if err != nil {
			log.Error("An error occurred while loading algorithm "+file+"; skipping.", "err", err)
			continue
		}

		h := sha256.New()
		binary.Write(h, binary.BigEndian, fileBinary)
		hash := h.Sum(nil)

		exists, err := i.IngressDB.ExistsAlgorithm(ctx, hash)
		if err != nil {
			log.Error("An error occurred while checking if algorithm exists "+file+"; skipping.", "err", err)
			continue
		}
		if exists {
			log.Info("Algorithm is already loaded in algorithm DB; " + file)
			continue
		}

		err = i.IngressDB.AddAlgorithm(ctx, hash, fileBinary)
		if err != nil {
			log.Error("An error occurred while adding algorithm "+file+"; skipping.", "err", err)
			continue
		}
		log.Info("Loaded algorithm into algorithm DB; " + file + "; " + hex.EncodeToString(hash))
		if alg.Fallback {
			hasFallback = true
			err = i.IngressDB.AddAlgorithm(ctx, []byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}, fileBinary)
			if err != nil {
				log.Error("An error occurred while adding algorithm "+file+"; skipping.", "err", err)
				continue
			}
		}
	}
	if !hasFallback {
		log.Error("No fallback algorithm is configured for RACs in this AS. This means the AS WILL NOT be backwards" +
			"compatible with older beaconing versions of SCION. Please configure a fallback algorithm to ensure connectivity.")
	}
}

func (i *IngressServer) GetAlgorithm(ctx context.Context, request *cppb.AlgorithmRequest) (*cppb.AlgorithmResponse, error) {
	code, err := i.IngressDB.GetAlgorithm(ctx, request.AlgorithmHash)
	if err != nil {
		return &cppb.AlgorithmResponse{}, err
	}
	return &cppb.AlgorithmResponse{Code: code}, nil
}

func (i *IngressServer) Handle(ctx context.Context, req *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	gPeer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, serrors.New("peer must exist")
	}
	logger := log.FromCtx(ctx)
	//logger.Debug("Received Beacon from " + fmt.Sprintf("%T", gPeer.Addr.(*snet.UDPAddr)) + ", msg:" + req.String())

	peer, ok := gPeer.Addr.(*snet.UDPAddr)
	if !ok {
		logger.Debug("peer must be *snet.UDPAddr", "actual", fmt.Sprintf("%T", gPeer))
		return nil, serrors.New("peer must be *snet.UDPAddr", "actual", fmt.Sprintf("%T", gPeer))
	}
	ingress, err := extractIngressIfID(peer.Path)
	if err != nil {
		logger.Debug("Failed to extract ingress interface", "peer", peer, "err", err)
		return nil, status.Error(codes.InvalidArgument, "failed to extract ingress interface")
	}
	ps, err := seg.FullBeaconFromPB(req.Segment)
	if err != nil {
		logger.Debug("Failed to parse beacon", "peer", peer, "err", err)
		return nil, status.Error(codes.InvalidArgument, "failed to parse beacon")
	}
	b := beacon.Beacon{
		Segment: ps,
		InIfID:  ingress,
	}
	if err := i.IncomingHandler.HandleBeacon(ctx, b, peer); err != nil {
		logger.Debug("Failed to handle beacon", "peer", peer, "err", err)
		return nil, serrors.WrapStr("handling beacon", err)
	}
	return &cppb.IncomingBeaconResponse{}, nil
}

func (i *IngressServer) GetJob(ctx context.Context, request *cppb.RACBeaconRequest) (*cppb.RACJob, error) {
	timeGetRacJobsS := time.Now()
	racJobsM, err := i.IngressDB.GetRacJobs(ctx, request.IgnoreIntfGroup)
	if err != nil {
		return &cppb.RACJob{}, err
	}
	if len(racJobsM) == 0 {
		return &cppb.RACJob{}, nil
	}
	timeGetRacJobsE := time.Now()
	//TODO: Implement the rac job selection logic using the factors

	// select a random job
	racJobM := racJobsM[rand.Intn(len(racJobsM))]
	timeGetRacJobS := time.Now()
	racJob, err := i.getRacJob(ctx, request, racJobM)
	if err != nil {
		return &cppb.RACJob{}, err
	}
	timeGetRacJobE := time.Now()
	log.Info("GJ; ", "RacJobs", timeGetRacJobsE.Sub(timeGetRacJobsS).Seconds(), "RacJob", timeGetRacJobE.Sub(timeGetRacJobS).Seconds())
	return racJob, nil
}

// Deprecated
func (i *IngressServer) BeaconSources(ctx context.Context, request *cppb.RACBeaconSourcesRequest) (*cppb.RACBeaconSources, error) {
	log.Info("request incoming for beacons", "req", request.IgnoreIntfGroup)

	beaconSources, err := i.IngressDB.BeaconSources(ctx, request.IgnoreIntfGroup)
	if err != nil {
		log.Error("error happened oh no", "err", err)
	}
	log.Info("beacon sources", "src", beaconSources)
	return &cppb.RACBeaconSources{Sources: beaconSources}, err
}

// extractIngressIfID extracts the ingress interface ID from a path.
func extractIngressIfID(path snet.DataplanePath) (uint16, error) {
	invertedPath, ok := path.(snet.RawReplyPath)
	if !ok {
		return 0, serrors.New("unexpected path", "type", common.TypeOf(path))
	}
	rawScionPath, ok := invertedPath.Path.(*scion.Raw)
	if !ok {
		return 0, serrors.New("unexpected path", "type", common.TypeOf(path))
	}
	hf, err := rawScionPath.GetCurrentHopField()
	if err != nil {
		return 0, serrors.WrapStr("getting current hop field", err)
	}
	return hf.ConsIngress, nil
}
