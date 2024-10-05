package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/scionproto/scion/private/procperf"

	"github.com/opentracing/opentracing-go"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/control/irec/egress"
	"github.com/scionproto/scion/control/irec/ingress/storage"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/snet"
	"github.com/scionproto/scion/pkg/snet/addrutil"
	infraenv "github.com/scionproto/scion/private/app/appnet"
	"github.com/scionproto/scion/private/segment/segverifier"
	infra "github.com/scionproto/scion/private/segment/verifier"
	"github.com/scionproto/scion/private/topology"
)

type Handler struct {
	LocalIA    addr.IA
	IngressDB  storage.IngressStore
	Verifier   infra.Verifier
	Interfaces *ifstate.Interfaces
	Rewriter   *infraenv.AddressRewriter
	Extender   *egress.DefaultExtender
	Pather     *addrutil.Pather
	Dialer     *libgrpc.QUICDialer
	Peers      []uint16
}

func (h Handler) HandleBeacon(ctx context.Context, b beacon.Beacon, peer *snet.UDPAddr) error {
	bcnId := procperf.GetFullId(b.Segment.GetLoggingID(), b.Segment.Info.SegmentID)
	pp := procperf.GetNew(procperf.ReceivedBcn, bcnId)
	defer pp.Write()

	span := opentracing.SpanFromContext(ctx)
	intf := h.Interfaces.Get(b.InIfID)
	if intf == nil {
		err := serrors.New("received beacon on non-existent interface",
			"ingress_interface", b.InIfID)
		return err
	}

	upstream := intf.TopoInfo().IA
	if span != nil {
		span.SetTag("ingress_interface", b.InIfID)
		span.SetTag("upstream", upstream)
	}
	logger := log.FromCtx(ctx).New("beacon", b, "upstream", upstream)
	ctx = log.CtxWith(ctx, logger)

	//logger.Debug("Received beacon", "bcn", b)
	// TODO(jvb); investigate whether the prefilter is desired.
	//if err := h.IngressDB.PreFilter(b); err != nil {
	//	logger.Debug("Beacon pre-filtered", "err", err)
	//	return err
	//}
	timeValidateS := time.Now()
	if err := h.validateASEntry(b, intf); err != nil {
		logger.Info("Beacon validation failed", "err", err)
		return err
	}
	timeValidateE := time.Now()
	pp.AddDurationT(timeValidateS, timeValidateE) // 0
	timeVerifyS := time.Now()
	if err := h.verifySegment(ctx, b.Segment, peer); err != nil {
		logger.Info("Beacon verification failed", "err", err)
		return serrors.WrapStr("verifying beacon", err)
	}
	if len(b.Segment.ASEntries) == 0 { // Should not happen
		logger.Info("Not enough AS entries to process")
		return serrors.New("Not enough AS entries to process")
	}
	timeVerifyE := time.Now()
	pp.AddDurationT(timeVerifyS, timeVerifyE) // 1
	timePreFilterS := time.Now()              // 3
	if err := h.IngressDB.PreFilter(b); err != nil {
		logger.Debug("Beacon pre-filtered", "err", err)
		return err
	}
	timePreFilterE := time.Now()
	pp.AddDurationT(timePreFilterS, timePreFilterE) // 2
	timeValidateAlgS := time.Now()
	// Check if all algorithm ids in the as entry extensions are equal
	// It is possible for hops to not have Irec.
	if err := h.validateAlgorithmHash(b.Segment); err != nil {
		logger.Info("Beacon verification failed", "err", err)
		return serrors.WrapStr("verifying beacon", err)
	}
	timeValidateAlgE := time.Now()
	pp.AddDurationT(timeValidateAlgS, timeValidateAlgE) // 3

	// Verification checks passed, now check if the algorithm is known
	go func() {
		defer log.HandlePanic()

		if err := h.checkAndFetchAlgorithm(context.Background(), &b, peer); err != nil {
			logger.Info("Retrieving algorithm failed", "err", err)
		}
	}()
	if b.Segment.MaxExpiry().Before(time.Now()) {
		logger.Debug("Skipping as beacon is expired")
		return nil
	}
	timeInsertS := time.Now()
	// Insert with algorithm id and origin intfgroup
	if _, err := h.IngressDB.InsertBeacon(ctx, b); err != nil {
		logger.Debug("Failed to insert beacon", "err", err)
		return serrors.WrapStr("inserting beacon", err)
	}
	timeInsertE := time.Now()
	pp.AddDurationT(timeInsertS, timeInsertE) // 4
	//logger.Debug("Inserted beacon")
	return nil
}

func (h Handler) checkAndFetchAlgorithm(ctx context.Context, b *beacon.Beacon, peer *snet.UDPAddr) error {
	algHash := egress.HashToString(b.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash)
	pp := procperf.GetNew(procperf.Algorithm, algHash)
	defer pp.Write()
	timeAlgCheckS := time.Now()
	exists, err := h.IngressDB.ExistsAlgorithm(ctx, b.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash)
	if err != nil {
		return serrors.WrapStr("Couldn't check whether the algorithm is in the database", err)
	}
	timeAlgCheckE := time.Now()
	pp.AddDurationT(timeAlgCheckS, timeAlgCheckE) // 0
	if exists {
		return nil
	}

	// If the algorithm does not exist, we need to contact the Origin AS. To do so we use the existing SCION combinator
	// to create a path back to the origin AS using the path described in the beacon.
	//TODO(jvb); Optimize this slightly by removing the need for the extender.
	segCopy, _ := seg.BeaconFromPB(seg.PathSegmentToPB(b.Segment))
	timeExtendS := time.Now()
	err = h.Extender.Extend(ctx, segCopy, b.InIfID, 0, false, nil, []uint16{})
	if err != nil {
		return err
	}
	timeExtendE := time.Now()
	pp.AddDurationT(timeExtendS, timeExtendE) // 1
	timePathS := time.Now()
	address, err := h.Pather.GetPath(addr.SvcCS, segCopy)
	if err != nil {
		log.Error("Unable to choose server", "err", err)
	}
	timePathE := time.Now()
	pp.AddDurationT(timePathS, timePathE) // 2
	timeDialS := time.Now()
	conn, err := h.Dialer.Dial(ctx, address)
	if err != nil {
		return serrors.WrapStr("Error occurred while dialing origin AS", err)
	}
	defer conn.Close()
	timeDialE := time.Now()
	pp.AddDurationT(timeDialS, timeDialE) // 3
	timeGrpcS := time.Now()
	client := cppb.NewIngressInterServiceClient(conn)
	alg, err := client.GetAlgorithm(ctx, &cppb.AlgorithmRequest{AlgorithmHash: b.Segment.ASEntries[0].Extensions.Irec.AlgorithmHash})
	if err != nil {
		return serrors.WrapStr("Error occurred while contacting origin as for algorithm", err)
	}
	// TODO(jvb); to prevent DoS, should limit the amount of attempts we do for an algorithm retrieval per minute, aka
	// enforce a delay.
	timeGrpcE := time.Now()
	pp.AddDurationT(timeGrpcS, timeGrpcE) // 4
	timeHashS := time.Now()
	hash := sha256.New()
	binary.Write(hash, binary.BigEndian, alg.Code)
	timeHashE := time.Now()
	pp.AddDurationT(timeHashS, timeHashE) // 5
	timeInsertS := time.Now()
	err = h.IngressDB.AddAlgorithm(ctx, hash.Sum(nil), alg.Code)
	if err != nil {
		return serrors.WrapStr("Error occurred while adding new algorithm", err)
	}
	timeInsertE := time.Now()
	pp.AddDurationT(timeInsertS, timeInsertE) // 6
	return nil
}

func (h Handler) validateAlgorithmHash(segment *seg.PathSegment) error {
	originASEntry := segment.ASEntries[0]
	if originASEntry.Extensions.Irec == nil {
		return nil
	}
	for _, asEntry := range segment.ASEntries {
		if asEntry.Extensions.Irec == nil {
			continue
		}
		if !bytes.Equal(asEntry.Extensions.Irec.AlgorithmHash, originASEntry.Extensions.Irec.AlgorithmHash) {
			return serrors.New("algorithm hash is different between AS entries")
		}
		if asEntry.Extensions.Irec.AlgorithmId != originASEntry.Extensions.Irec.AlgorithmId {
			return serrors.New("algorithm id is different between AS entries")
		}
	}
	return nil
}

func (h Handler) validateASEntry(b beacon.Beacon, intf *ifstate.Interface) error {
	topoInfo := intf.TopoInfo()
	if topoInfo.LinkType != topology.Parent && topoInfo.LinkType != topology.Core {
		return serrors.New("beacon received on invalid link",
			"ingress_interface", b.InIfID, "link_type", topoInfo.LinkType)
	}
	asEntry := b.Segment.ASEntries[b.Segment.MaxIdx()]
	if !asEntry.Local.Equal(topoInfo.IA) {
		return serrors.New("invalid upstream ISD-AS",
			"expected", topoInfo.IA, "actual", asEntry.Local)
	}
	if !asEntry.Next.Equal(h.LocalIA) {
		return serrors.New("next ISD-AS of upstream AS entry does not match local ISD-AS",
			"expected", h.LocalIA, "actual", asEntry.Next)
	}
	return nil
}

func (h Handler) verifySegment(ctx context.Context, segment *seg.PathSegment,
	peer *snet.UDPAddr) error {

	peerPath, err := peer.GetPath()
	if err != nil {
		return err
	}
	svcToQuery := &snet.SVCAddr{
		IA:      peer.IA,
		Path:    peerPath.Dataplane(),
		NextHop: peerPath.UnderlayNextHop(),
		SVC:     addr.SvcCS,
	}
	return segverifier.VerifySegment(ctx, h.Verifier, svcToQuery, segment)
}
