//go:build !wa && !waopt && !native

package storage

import (
	"context"
	"time"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	beacon2 "github.com/scionproto/scion/private/storage/ingress"
)

type IngressStore interface {
	GetBeacons(ctx context.Context, req *cppb.BeaconQuery) ([][]byte, []*cppb.IRECBeaconUnopt, error)
	GetBeaconJob(ctx context.Context, ignoreIntfGroup bool) ([][]byte, []*cppb.IRECBeaconUnopt, []byte, []int64, error)
	GetBeaconsByRowIDs(ctx context.Context, ids []int64) ([]*cppb.EgressBeacon, error)
	GetBeaconByRowID(ctx context.Context, id int64) (*cppb.EgressBeacon, error)

	MarkBeacons(ctx context.Context, ids []int64) error
	InsertBeacon(ctx context.Context, b beacon.Beacon) (beacon.InsertStats, error)

	AddAlgorithm(ctx context.Context, algorithmHash []byte, algorithmCode []byte) error
	GetAlgorithm(ctx context.Context, algorithmHash []byte) ([]byte, error)
	ExistsAlgorithm(ctx context.Context, algorithmHash []byte) (bool, error)
	//DEPRECATED;
	GetAndMarkBeacons(ctx context.Context, req *cppb.RACBeaconRequest) ([][]byte, []*cppb.IRECBeaconUnopt, error)
	BeaconSources(ctx context.Context, ignoreIntfGroup bool) ([]*cppb.RACBeaconSource, error)
	PreFilter(b beacon.Beacon) error
}

// Beaconstore
type Store struct {
	baseStore
	policies beacon.Policies
}

func (s *Store) GetBeaconsByRowIDs(ctx context.Context, ids []int64) ([]*cppb.EgressBeacon, error) {
	return s.db.GetBeaconsByRowIDs(ctx, ids)
}

func (s *Store) GetBeaconByRowID(ctx context.Context, id int64) (*cppb.EgressBeacon, error) {
	return s.db.GetBeaconByRowID(ctx, id)
}

// NewBeaconStore creates a new beacon store for the ingress gateway
func NewIngressDB(policies beacon.Policies, db DB) (*Store, error) {
	policies.InitDefaults()
	if err := policies.Validate(); err != nil {
		return nil, err
	}
	s := &Store{
		baseStore: baseStore{
			db: db,
		},
		policies: policies,
	}
	s.baseStore.usager = &s.policies
	return s, nil
}

type usager interface {
	Filter(beacon beacon.Beacon) error
	Usage(beacon beacon.Beacon) beacon.Usage
}

type BeaconSource struct {
	IA            addr.IA
	IntfGroup     uint16
	AlgorithmHash []byte
	AlgorithmId   uint32
}

// DB defines the interface that all beacon DB backends have to implement.
type DB interface {
	GetAndMarkBeacons(ctx context.Context, maximum uint32, algHash []byte, algID uint32, originAS addr.IA, originIntfGroup uint32, ignoreIntfGroup bool, marker uint32) ([][]byte, []*cppb.IRECBeaconUnopt, error)
	BeaconSources(ctx context.Context, ignoreIntfGroup bool) ([]*cppb.RACBeaconSource, error)
	InsertBeacon(ctx context.Context, b beacon.Beacon, usage beacon.Usage) (beacon.InsertStats, error)

	MarkBeacons(ctx context.Context, ids []int64) error
	GetBeaconJob(ctx context.Context, ignoreIntfGroup bool, fetchExpirationTime time.Time) ([][]byte, []*cppb.IRECBeaconUnopt, []byte, []int64, error)
	GetBeacons(ctx context.Context, opts *beacon2.QueryOptions) ([][]byte, []*cppb.IRECBeaconUnopt, error)
	GetBeaconsByRowIDs(ctx context.Context, ids []int64) ([]*cppb.EgressBeacon, error)
	GetBeaconByRowID(ctx context.Context, id int64) (*cppb.EgressBeacon, error)

	AddAlgorithm(ctx context.Context, algorithmHash []byte, algorithmCode []byte) error
	GetAlgorithm(ctx context.Context, algorithmHash []byte) ([]byte, error)
	ExistsAlgorithm(ctx context.Context, algorithmHash []byte) (bool, error)
}

type baseStore struct {
	db     DB
	usager usager
}

func (s *baseStore) PreFilter(beacon beacon.Beacon) error {
	return s.usager.Filter(beacon)
}

func (s *baseStore) MarkBeacons(ctx context.Context, ids []int64) error {
	return s.db.MarkBeacons(ctx, ids)
}

func (s *baseStore) InsertBeacon(ctx context.Context, b beacon.Beacon) (beacon.InsertStats, error) {
	usage := s.usager.Usage(b)
	if usage.None() {
		return beacon.InsertStats{Filtered: 1}, nil
	}
	return s.db.InsertBeacon(ctx, b, usage)
}

func (s *baseStore) GetAndMarkBeacons(ctx context.Context, req *cppb.RACBeaconRequest) ([][]byte, []*cppb.IRECBeaconUnopt, error) {
	fbs, bcns, err := s.db.GetAndMarkBeacons(ctx, req.Maximum, req.AlgorithmHash, req.AlgorithmID, addr.IA(req.OriginAS), req.OriginIntfGroup, req.IgnoreIntfGroup, 1)
	if err != nil {
		return nil, nil, serrors.WrapStr("retrieving beacons failed", err)
	}

	return fbs, bcns, nil
}

func (s *baseStore) GetBeaconJob(ctx context.Context, ignoreIntfGroup bool) ([][]byte, []*cppb.IRECBeaconUnopt, []byte, []int64, error) {
	return s.db.GetBeaconJob(ctx, ignoreIntfGroup, time.Now().Add(time.Second*30))
}

func (s *baseStore) GetBeacons(ctx context.Context, req *cppb.BeaconQuery) ([][]byte, []*cppb.IRECBeaconUnopt, error) {
	algorithms := make([]beacon2.AlgorithmOption, len(req.Algorithms))
	for i, alg := range req.Algorithms {
		algorithms[i] = beacon2.AlgorithmOption{
			AlgHash: alg.AlgHash,
			AlgId:   alg.AlgID,
		}
	}
	origins := make([]beacon2.OriginOption, len(req.Origins))
	for i, origin := range req.Origins {
		origins[i] = beacon2.OriginOption{
			OriginAS:        addr.IA(origin.OriginAS),
			OriginIntfGroup: origin.OriginIntfGroup,
		}
	}
	fbs, bcns, err := s.db.GetBeacons(ctx, &beacon2.QueryOptions{
		Maximum:      req.Maximum,
		Algorithms:   algorithms,
		Origins:      origins,
		OnlyUnmarked: req.OnlyUnmarked,
		Labels:       req.Labels,
	})
	if err != nil {
		return [][]byte{}, []*cppb.IRECBeaconUnopt{}, serrors.WrapStr("retrieving beacons failed", err)
	}

	return fbs, bcns, nil
}

func (s *baseStore) BeaconSources(ctx context.Context, ignoreIntfGroup bool) ([]*cppb.RACBeaconSource, error) {
	return s.db.BeaconSources(ctx, ignoreIntfGroup)
}

func (s *baseStore) AddAlgorithm(ctx context.Context, algorithmHash []byte, algorithmCode []byte) error {
	return s.db.AddAlgorithm(ctx, algorithmHash, algorithmCode)
}
func (s *baseStore) GetAlgorithm(ctx context.Context, algorithmHash []byte) ([]byte, error) {
	return s.db.GetAlgorithm(ctx, algorithmHash)
}
func (s *baseStore) ExistsAlgorithm(ctx context.Context, algorithmHash []byte) (bool, error) {
	return s.db.ExistsAlgorithm(ctx, algorithmHash)
}
func (s *Store) MaxExpTime(policyType beacon.PolicyType) uint8 {
	switch policyType {
	case beacon.UpRegPolicy:
		return *s.policies.UpReg.MaxExpTime
	case beacon.DownRegPolicy:
		return *s.policies.DownReg.MaxExpTime
	case beacon.PropPolicy:
		return *s.policies.Prop.MaxExpTime
	}
	return beacon.DefaultMaxExpTime
}
