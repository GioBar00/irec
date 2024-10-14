package egress_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"github.com/scionproto/scion/control/beaconing"
	"net"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/control/irec/egress"
	"github.com/scionproto/scion/control/irec/egress/mock_egress"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/private/common"
	"github.com/scionproto/scion/pkg/private/xtest/graph"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/slayers/path/scion"
	"github.com/scionproto/scion/pkg/snet"
	"github.com/scionproto/scion/pkg/snet/addrutil"
	snetpath "github.com/scionproto/scion/pkg/snet/path"
	"github.com/scionproto/scion/private/segment/seghandler"
	"github.com/scionproto/scion/private/topology"
)

func TestRegistrarRun(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()

	testsLocal := []struct {
		name          string
		segType       seg.Type
		fn            string
		beacons       [][]uint16
		inactivePeers map[uint16]bool
	}{
		{
			name:    "Core segment",
			segType: seg.TypeCore,
			fn:      topoCore,
			beacons: [][]uint16{
				{graph.If_120_A_110_X},
				{graph.If_130_B_120_A, graph.If_120_A_110_X},
			},
		},
		{
			name:          "Up segment",
			segType:       seg.TypeUp,
			fn:            topoNonCore,
			inactivePeers: map[uint16]bool{graph.If_111_C_121_X: true},
			beacons: [][]uint16{
				{graph.If_120_X_111_B},
				{graph.If_130_B_120_A, graph.If_120_X_111_B},
			},
		},
	}
	for _, test := range testsLocal {
		t.Run(test.name, func(t *testing.T) {
			mctrl := gomock.NewController(t)
			defer mctrl.Finish()
			topo, err := topology.FromJSONFile(test.fn)
			require.NoError(t, err)
			intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})

			segStore := mock_egress.NewMockSegmentStore(mctrl)
			writer := &egress.LocalWriter{
				Extender: &beaconing.DefaultExtender{
					IA:         topo.IA(),
					MTU:        topo.MTU(),
					Signer:     testSigner(t, priv, topo.IA()),
					Intfs:      intfs,
					MAC:        macFactory,
					MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
					StaticInfo: func() *beaconing.StaticInfoCfg { return nil },
				},
				Intfs: intfs,
				Store: segStore,
				Type:  test.segType,
			}
			g := graph.NewDefaultGraph(mctrl)
			bcns := make([]beacon.Beacon, 0, len(test.beacons))
			for _, desc := range test.beacons {
				bcns = append(bcns, testBeacon(g, desc, false))
			}
			var stored []*seg.Meta
			segStore.EXPECT().StoreSegs(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, segs []*seg.Meta) (seghandler.SegStats, error) {
					stored = append(stored, segs...)
					var inserted []string
					for _, seg := range segs {
						inserted = append(inserted, seg.Segment.GetLoggingID())
					}
					return seghandler.SegStats{InsertedSegs: inserted}, nil
				},
			)

			writer.Write(context.Background(), bcns, []uint16{}, true)
			assert.Len(t, stored, len(test.beacons))

			for _, s := range stored {
				assert.NoError(t, s.Segment.Validate(seg.ValidateSegment))
				assert.NoError(t, s.Segment.VerifyASEntry(context.Background(),
					segVerifier{pubKey: pub}, s.Segment.MaxIdx()))
				assert.Equal(t, test.segType, s.Type)
			}

		})
	}
	testsRemote := []struct {
		name          string
		segType       seg.Type
		fn            string
		beacons       [][]uint16
		inactivePeers map[uint16]bool
	}{
		{
			name:          "Down segment",
			segType:       seg.TypeDown,
			fn:            topoNonCore,
			inactivePeers: map[uint16]bool{graph.If_111_C_121_X: true},
			beacons: [][]uint16{
				{graph.If_120_X_111_B},
				{graph.If_130_B_120_A, graph.If_120_X_111_B},
			},
		},
	}
	for _, test := range testsRemote {
		t.Run(test.name, func(t *testing.T) {
			mctrl := gomock.NewController(t)
			defer mctrl.Finish()

			topo, err := topology.FromJSONFile(test.fn)
			require.NoError(t, err)

			intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
			rpc := mock_egress.NewMockRPC(mctrl)
			writer := &egress.RemoteWriter{
				Extender: &beaconing.DefaultExtender{
					IA:         topo.IA(),
					MTU:        topo.MTU(),
					Signer:     testSigner(t, priv, topo.IA()),
					Intfs:      intfs,
					MAC:        macFactory,
					MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
					StaticInfo: func() *beaconing.StaticInfoCfg { return nil },
				},
				Pather: addrutil.Pather{
					NextHopper: topoWrap{Topo: topo},
				},
				RPC:   rpc,
				Type:  test.segType,
				Intfs: intfs,
			}

			g := graph.NewDefaultGraph(mctrl)
			bcns := make([]beacon.Beacon, 0, len(test.beacons))
			for _, desc := range test.beacons {
				bcns = append(bcns, testBeacon(g, desc, false))
			}

			type regMsg struct {
				Meta seg.Meta
				Addr *snet.SVCAddr
			}
			segMu := sync.Mutex{}
			var sent []regMsg

			// Collect the segments that are sent on the messenger.
			rpc.EXPECT().RegisterSegment(gomock.Any(), gomock.Any(),
				gomock.Any()).Times(len(test.beacons)).DoAndReturn(
				func(_ context.Context, meta seg.Meta, remote net.Addr) error {
					segMu.Lock()
					defer segMu.Unlock()
					sent = append(sent, regMsg{
						Meta: meta,
						Addr: remote.(*snet.SVCAddr),
					})
					return nil
				},
			)
			writer.Write(context.Background(), bcns, []uint16{}, true)

			require.Len(t, sent, len(test.beacons))
			for segIdx, s := range sent {
				t.Run(fmt.Sprintf("seg idx %d", segIdx), func(t *testing.T) {
					pseg := s.Meta.Segment

					assert.NoError(t, pseg.Validate(seg.ValidateSegment))
					assert.NoError(t, pseg.VerifyASEntry(context.Background(),
						segVerifier{pubKey: pub}, pseg.MaxIdx()))

					assert.Equal(t, pseg.FirstIA(), s.Addr.IA)
					assert.Equal(t, addr.SvcCS, s.Addr.SVC)

					var path scion.Decoded
					scionPath, ok := s.Addr.Path.(snetpath.SCION)
					require.True(t, ok)
					if assert.NoError(t, path.DecodeFromBytes(scionPath.Raw)) {
						pathHopField := path.HopFields[0]

						segHopField := pseg.ASEntries[pseg.MaxIdx()].HopEntry.HopField
						assert.Equal(t, pathHopField.Mac, segHopField.MAC)
						assert.Equal(t, pathHopField.ConsIngress, segHopField.ConsIngress)
						assert.Equal(t, pathHopField.ConsEgress, segHopField.ConsEgress)

						nextHop := pathHopField.ConsIngress
						a := net.UDPAddrFromAddrPort(interfaceInfos(topo)[nextHop].InternalAddr)
						assert.Equal(t, a, s.Addr.NextHop)
					}
				})
			}
		})
	}

	t.Run("only extend when requested", func(t *testing.T) {
		mctrl := gomock.NewController(t)
		defer mctrl.Finish()

		topo, err := topology.FromJSONFile(topoNonCore)
		require.NoError(t, err)

		intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
		segStore := mock_egress.NewMockSegmentStore(mctrl)

		writer := &egress.LocalWriter{
			Extender: &beaconing.DefaultExtender{
				IA:         topo.IA(),
				MTU:        topo.MTU(),
				Signer:     testSigner(t, priv, topo.IA()),
				Intfs:      intfs,
				MAC:        macFactory,
				MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
				StaticInfo: func() *beaconing.StaticInfoCfg { return nil },
			},
			Intfs: intfs,
			Store: segStore,
			Type:  seg.TypeCoreR,
		}

		g := graph.NewDefaultGraph(mctrl)
		bcns := []beacon.Beacon{testBeacon(g, []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B}, false)}
		t.Run("not extended", func(t *testing.T) {
			ia, err := addr.ParseIA("1-ff00:0:120")
			require.NoError(t, err)
			// Collect the segments that are sent on the messenger.
			segStore.EXPECT().StoreSegs(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, segs []*seg.Meta) (seghandler.SegStats, error) {
					for _, seg := range segs {

						assert.Equal(t, ia, seg.Segment.ASEntries[seg.Segment.MaxIdx()].Local)

					}
					return seghandler.SegStats{}, nil
				},
			)
			err = writer.Write(context.Background(), bcns, []uint16{}, false)
			require.NoError(t, err)
		})
		t.Run("extended", func(t *testing.T) {
			ia, err := addr.ParseIA("1-ff00:0:111")
			require.NoError(t, err)
			// Collect the segments that are sent on the messenger.
			segStore.EXPECT().StoreSegs(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, segs []*seg.Meta) (seghandler.SegStats, error) {
					for _, seg := range segs {

						assert.Equal(t, ia, seg.Segment.ASEntries[seg.Segment.MaxIdx()].Local)

					}
					return seghandler.SegStats{}, nil
				},
			)
			err = writer.Write(context.Background(), bcns, []uint16{}, true)
			require.NoError(t, err)
		})

	})

}

func testBeacon(g *graph.Graph, desc []uint16, addIrec bool) beacon.Beacon {
	var bseg *seg.PathSegment
	if addIrec {

		bseg = g.BeaconWithIRECExtension(desc)
	} else {
		bseg = g.Beacon(desc)
	}
	asEntry := bseg.ASEntries[bseg.MaxIdx()]
	bseg.ASEntries = bseg.ASEntries[:len(bseg.ASEntries)-1]

	return beacon.Beacon{
		InIfID:  asEntry.HopEntry.HopField.ConsIngress,
		Segment: bseg,
	}
}

type topoWrap struct {
	Topo topology.Topology
}

func (w topoWrap) UnderlayNextHop(id uint16) *net.UDPAddr {
	a, _ := w.Topo.UnderlayNextHop(common.IfIDType(id))
	return a
}
