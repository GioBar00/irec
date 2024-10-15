package egress_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/scionproto/scion/control/beaconing"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/control/irec/egress"
	"github.com/scionproto/scion/control/irec/egress/mock_egress"
	"github.com/scionproto/scion/control/irec/egress/storage/mock_egress_storage"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/private/xtest/graph"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/segment/extensions/irec"
	"github.com/scionproto/scion/pkg/snet/addrutil"
	"github.com/scionproto/scion/private/topology"
)

func TestPropagatorRunNonCore(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()
	type beaconInfo struct {
		desc       []uint16
		egressIntf []uint32
		inIntfId   uint32
	}
	testCases := []struct {
		beacons []beaconInfo

		egressIntfs               [][]uint32
		name                      string
		runCalls                  int
		expectedPropagatedBeacons int
		expectedPropagationChecks int
		expectedWrites            int
		inIntfId                  []uint32
	}{
		{
			name:                      "beacons are propagated and written to local path db",
			runCalls:                  1,
			expectedPropagatedBeacons: 3,
			expectedPropagationChecks: 3,
			expectedWrites:            3,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723}, // only 1 intf is valid
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723}, // only 1 intf is valid
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723}, // only 1 intf is valid
					inIntfId:   uint32(2712),
				},
			},
		},
		{
			name:                      "similar beacons are only propagated once",
			runCalls:                  4,
			expectedPropagatedBeacons: 3,
			expectedPropagationChecks: 3 * 4,
			expectedWrites:            3 * 4,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723},
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723},
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{1417, 2712, 2723},
					inIntfId:   uint32(2712),
				},
			},
		},
		{
			name:                      "incorrect interface ID's are filtered",
			runCalls:                  1,
			expectedPropagatedBeacons: 0,
			expectedPropagationChecks: 0,
			expectedWrites:            2,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_X_111_B},
					egressIntf: []uint32{1111, 2222, 3333, 4444, 1, 2},
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{1111, 2222, 3333, 4444, 1, 2},
					inIntfId:   uint32(2712),
				},
			},
		},
		{
			name:                      "cannot prop on parent or peer links",
			runCalls:                  1,
			expectedPropagatedBeacons: 0,
			expectedPropagationChecks: 0,
			expectedWrites:            2,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_X_111_B},
					egressIntf: []uint32{2815, 2712, 2723, 2823},
					inIntfId:   uint32(2712),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B},
					egressIntf: []uint32{2815, 2712, 2723, 2823},
					inIntfId:   uint32(2712),
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			mctrl := gomock.NewController(t)
			defer mctrl.Finish()
			topo, err := topology.FromJSONFile(topoNonCore)
			require.NoError(t, err)
			intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
			senderFactory := mock_egress.NewMockSenderFactory(mctrl)
			storage := mock_egress_storage.NewMockEgressDB(mctrl)
			writer := mock_egress.NewMockWriter(mctrl)

			writer.EXPECT().WriterType().AnyTimes().Return(seg.TypeCore)
			writer.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedWrites).DoAndReturn(func(_, _, _, _ interface{}) error {
				return nil
			})
			hashList := make([]string, 0)
			filter := func(intf *ifstate.Interface) bool {
				return intf.TopoInfo().LinkType == topology.Child
			}
			intfMap := make(map[uint32]*ifstate.Interface)
			for _, intf := range intfs.Filtered(filter) {
				intfMap[uint32(intf.TopoInfo().ID)] = intf
			}
			p := egress.Propagator{
				Extender: &egress.DefaultExtender{
					IA:         topo.IA(),
					MTU:        topo.MTU(),
					Signer:     testSigner(t, priv, topo.IA()),
					Intfs:      intfs,
					MAC:        macFactory,
					MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
					StaticInfo: func() *egress.StaticInfoCfg { return nil },
				},
				Core: topo.Core(),
				Pather: &addrutil.Pather{
					NextHopper: beaconing.topoWrap{topo},
				},
				Local:             topo.IA(),
				AllInterfaces:     intfs,
				PropagationFilter: filter,
				PropagationInterfaces: func() []*ifstate.Interface {
					return intfs.Filtered(filter)
				},
				Interfaces:    intfMap,
				SenderFactory: senderFactory,
				Store:         storage,
				Writers:       []beaconing.Writer{writer},
			}
			g := graph.NewDefaultGraph(mctrl)

			bcns := make([]*cppb.EgressBeacon, 0, len(test.beacons))
			for _, bcn := range test.beacons {
				bcns = append(bcns, &cppb.EgressBeacon{
					PathSeg:     seg.PathSegmentToPB(beaconing.testBeacon(g, bcn.desc, false).Segment),
					InIfId:      bcn.inIntfId,
					EgressIntfs: bcn.egressIntf, // The second and third egress interface are not a core or child link and should be ignored.
				})
			}

			senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(
				func(_ context.Context, _ addr.IA, egIfId uint16,
					nextHop *net.UDPAddr) (egress.Sender, error) {

					sender := mock_egress.NewMockSender(mctrl)
					sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(_ context.Context, b *seg.PathSegment) error {
							validateSend(t, b, egIfId, nextHop, pub, topo)
							return nil
						},
					)
					sender.EXPECT().Close().Times(1)

					return sender, nil
				},
			)

			storage.EXPECT().IsBeaconAlreadyPropagated(gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagationChecks).DoAndReturn(func(arg0 context.Context, arg1 []byte, arg2 *ifstate.Interface) (bool, int, error) {
				return slices.Contains(hashList, string(arg1)), 0, nil
			})

			storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(func(arg0 context.Context, arg1 []byte, intf *ifstate.Interface, expiry time.Time) error {
				hashList = append(hashList, string(arg1))
				return nil

			})
			//storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			for i := 0; i < test.runCalls; i++ {
				_, err = p.RequestPropagation(context.Background(), &cppb.PropagationRequest{Beacon: bcns})
				require.NoError(t, err)
			}
		})
	}
}
func TestPropagatorRunCore(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()
	type beaconInfo struct {
		desc       []uint16
		egressIntf []uint32
		inIntfId   uint32
	}
	testCases := []struct {
		beacons []beaconInfo

		egressIntfs               [][]uint32
		name                      string
		runCalls                  int
		expectedPropagatedBeacons int
		expectedPropagationChecks int
		expectedWrites            int
		inIntfId                  []uint32
	}{
		{
			name:                      "beacons are propagated and written to local path db",
			runCalls:                  1,
			expectedPropagatedBeacons: 2, // 2 beacons on 2 interfaces, 1 beacon to 1 intf.
			// but 2 beacons are not propagated, due to loop (1113 egress)
			expectedPropagationChecks: 2, // 2 beacons on 2 interfaces, 1 beacon to 1 intf, but 2 not propagated
			expectedWrites:            3,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X},
					egressIntf: []uint32{1113, 1129},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X},
					egressIntf: []uint32{1113, 1129},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X}, // this will be different to the beacon above, due to the randomized segID
					egressIntf: []uint32{1113},
					inIntfId:   uint32(1121),
				},
			},
		},
		{
			name:                      "similar beacons are only propagated once",
			runCalls:                  4,
			expectedPropagatedBeacons: 3 * 2,
			expectedPropagationChecks: 3 * 2 * 4, // 3 beacons, 2 interfaces for 4 runcalls.
			expectedWrites:            3 * 4,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
			},
		},
		{
			name:                      "incorrect interface ID's are filtered",
			runCalls:                  1,
			expectedPropagatedBeacons: 0,
			expectedPropagationChecks: 0,
			expectedWrites:            2,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_A_110_X},
					egressIntf: []uint32{1111, 1000, 8888},
					inIntfId:   uint32(55),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{1111, 2222, 3333, 4444, 1, 2},
					inIntfId:   uint32(1121),
				},
			},
		},
		{
			name:                      "only propagate on core links",
			runCalls:                  1,
			expectedPropagatedBeacons: 0,
			expectedPropagationChecks: 0,
			expectedWrites:            2,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_A_110_X},
					egressIntf: []uint32{42},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{42},
					inIntfId:   uint32(1121),
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			mctrl := gomock.NewController(t)
			defer mctrl.Finish()
			topo, err := topology.FromJSONFile(topoCore)
			require.NoError(t, err)
			intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
			senderFactory := mock_egress.NewMockSenderFactory(mctrl)
			storage := mock_egress_storage.NewMockEgressDB(mctrl)
			writer := mock_egress.NewMockWriter(mctrl)

			writer.EXPECT().WriterType().AnyTimes().Return(seg.TypeCore)
			writer.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedWrites).DoAndReturn(func(_, _, _, _ interface{}) error {
				return nil
			})
			hashList := make([]string, 0)
			filter := func(intf *ifstate.Interface) bool {
				return intf.TopoInfo().LinkType == topology.Core
			}
			intfMap := make(map[uint32]*ifstate.Interface)
			for _, intf := range intfs.Filtered(filter) {
				intfMap[uint32(intf.TopoInfo().ID)] = intf
			}
			p := egress.Propagator{
				Extender: &egress.DefaultExtender{
					IA:         topo.IA(),
					MTU:        topo.MTU(),
					Signer:     testSigner(t, priv, topo.IA()),
					Intfs:      intfs,
					MAC:        macFactory,
					MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
					StaticInfo: func() *egress.StaticInfoCfg { return nil },
				},
				Core: topo.Core(),
				Pather: &addrutil.Pather{
					NextHopper: beaconing.topoWrap{topo},
				},
				Local:             topo.IA(),
				AllInterfaces:     intfs,
				PropagationFilter: filter,
				PropagationInterfaces: func() []*ifstate.Interface {
					return intfs.Filtered(filter)
				},
				Interfaces:    intfMap,
				SenderFactory: senderFactory,
				Store:         storage,
				Writers:       []beaconing.Writer{writer},
			}
			g := graph.NewDefaultGraph(mctrl)

			bcns := make([]*cppb.EgressBeacon, 0, len(test.beacons))
			for _, bcn := range test.beacons {
				bcns = append(bcns, &cppb.EgressBeacon{
					PathSeg:     seg.PathSegmentToPB(beaconing.testBeacon(g, bcn.desc, false).Segment),
					InIfId:      bcn.inIntfId,
					EgressIntfs: bcn.egressIntf, // The second and third egress interface are not a core or child link and should be ignored.
				})
			}

			senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(
				func(_ context.Context, _ addr.IA, egIfId uint16,
					nextHop *net.UDPAddr) (egress.Sender, error) {

					sender := mock_egress.NewMockSender(mctrl)
					sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(_ context.Context, b *seg.PathSegment) error {
							validateSend(t, b, egIfId, nextHop, pub, topo)
							return nil
						},
					)
					sender.EXPECT().Close().Times(1)

					return sender, nil
				},
			)

			storage.EXPECT().IsBeaconAlreadyPropagated(gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagationChecks).DoAndReturn(func(arg0 context.Context, arg1 []byte, arg2 *ifstate.Interface) (bool, int, error) {
				return slices.Contains(hashList, strconv.Itoa(int(arg2.TopoInfo().ID))+string(arg1)), 0, nil
			})

			storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(func(arg0 context.Context, arg1 []byte, intf *ifstate.Interface, expiry time.Time) error {
				hashList = append(hashList, strconv.Itoa(int(intf.TopoInfo().ID))+string(arg1))
				fmt.Println(hex.EncodeToString(arg1))
				return nil

			})
			//storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			for i := 0; i < test.runCalls; i++ {
				_, err = p.RequestPropagation(context.Background(), &cppb.PropagationRequest{Beacon: bcns})
				require.NoError(t, err)
			}
		})
	}
}

// Test whether a pull based beacon is flooded on all interfaces
func TestPullBasedCore(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()

	mctrl := gomock.NewController(t)
	defer mctrl.Finish()
	topo, err := topology.FromJSONFile(topoCore)
	require.NoError(t, err)
	intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
	senderFactory := mock_egress.NewMockSenderFactory(mctrl)
	storage := mock_egress_storage.NewMockEgressDB(mctrl)
	writer := mock_egress.NewMockWriter(mctrl)

	originationFilter := func(intf *ifstate.Interface) bool {
		topoInfo := intf.TopoInfo()
		if topoInfo.LinkType == topology.Core || topoInfo.LinkType == topology.Child {
			return true
		}
		return false
	}
	signer := testSigner(t, priv, topo.IA())
	bo := egress.BasicOriginator{
		OriginatePerIntfGroup: true,
		Extender: &egress.DefaultExtender{
			IA:         topo.IA(),
			Signer:     signer,
			MAC:        macFactory,
			Intfs:      intfs,
			MTU:        topo.MTU(),
			MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
			Task:       "Test",
			StaticInfo: func() *egress.StaticInfoCfg { return nil },
			EPIC:       false,
		},
		SenderFactory: senderFactory,
		IA:            topo.IA(),
		Intfs:         intfs.Filtered(originationFilter),
	}
	pbo := egress.PullBasedOriginator{
		BasicOriginator: &bo,
	}
	filter := func(intf *ifstate.Interface) bool {
		return intf.TopoInfo().LinkType == topology.Core
	}
	intfMap := make(map[uint32]*ifstate.Interface)
	for _, intf := range intfs.Filtered(filter) {
		intfMap[uint32(intf.TopoInfo().ID)] = intf
	}
	p := egress.Propagator{
		Extender: &egress.DefaultExtender{
			IA:         topo.IA(),
			MTU:        topo.MTU(),
			Signer:     testSigner(t, priv, topo.IA()),
			Intfs:      intfs,
			MAC:        macFactory,
			MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
			StaticInfo: func() *egress.StaticInfoCfg { return nil },
		},
		Core: topo.Core(),
		Pather: &addrutil.Pather{
			NextHopper: beaconing.topoWrap{topo},
		},
		Local:             topo.IA(),
		AllInterfaces:     intfs,
		PropagationFilter: filter,
		PropagationInterfaces: func() []*ifstate.Interface {
			return intfs.Filtered(filter)
		},
		Interfaces:    intfMap,
		SenderFactory: senderFactory,
		Store:         storage,
		Writers:       []beaconing.Writer{writer},
		Originator:    &pbo,
	}
	pathRequest := &cppb.PullPathsRequest{
		DestinationIsdAs: uint64(topo.IFInfoMap()[0].IA),
		AlgorithmHash:    []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		AlgorithmId:      1,
	}
	senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(len(intfs.Filtered(originationFilter))).DoAndReturn(
		func(_ context.Context, _ addr.IA, egIfId uint16,
			nextHop *net.UDPAddr) (egress.Sender, error) {
			sender := mock_egress.NewMockSender(mctrl)
			sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(max(len(intfs.Get(egIfId).
				TopoInfo().Groups), 1)).DoAndReturn(
				func(_ context.Context, b *seg.PathSegment) error {
					validateSend(t, b, egIfId, nextHop, pub, topo)
					assert.NotNil(t, b.ASEntries[0].Extensions.Irec)
					assert.True(t, b.ASEntries[0].Extensions.Irec.PullBased)
					assert.Equal(t, uint64(b.ASEntries[0].Extensions.Irec.PullBasedTarget), pathRequest.DestinationIsdAs)
					assert.Equal(t, b.ASEntries[0].Extensions.Irec.AlgorithmHash, pathRequest.AlgorithmHash)
					assert.Equal(t, b.ASEntries[0].Extensions.Irec.AlgorithmId, pathRequest.AlgorithmId)
					return nil
				},
			)
			sender.EXPECT().Close().Times(1)

			return sender, nil
		},
	)
	_, err = p.RequestPullBasedOrigination(context.Background(), pathRequest)
	require.NoError(t, err)
}
func TestPropagatorAddsIrec(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()
	type beaconInfo struct {
		desc       []uint16
		egressIntf []uint32
		inIntfId   uint32
	}
	testCases := []struct {
		beacons []beaconInfo

		egressIntfs               [][]uint32
		name                      string
		runCalls                  int
		expectedPropagatedBeacons int
		expectedPropagationChecks int
		expectedWrites            int
		inIntfId                  []uint32
	}{
		{
			name:                      "Case 1",
			runCalls:                  1,
			expectedPropagatedBeacons: 5, // 2 beacons on 2 interfaces, 1 beacon to 1 intf
			expectedPropagationChecks: 5, // 2 beacons on 2 interfaces, 1 beacon to 1 intf
			expectedWrites:            3,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_111_A_130_B, graph.If_130_A_110_X}, // this will be different to the beacon above, due to the randomized segID
					egressIntf: []uint32{2113},
					inIntfId:   uint32(1121),
				},
			},
		},
		{
			name:                      "Similar beacons",
			runCalls:                  4,
			expectedPropagatedBeacons: 3 * 2,
			expectedPropagationChecks: 3 * 2 * 4, // 3 beacons, 2 interfaces for 4 runcalls.
			expectedWrites:            3 * 4,
			beacons: []beaconInfo{
				{desc: []uint16{graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
				{desc: []uint16{graph.If_130_B_120_A, graph.If_120_A_110_X},
					egressIntf: []uint32{2113, 3113},
					inIntfId:   uint32(1121),
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {

			mctrl := gomock.NewController(t)
			defer mctrl.Finish()
			topo, err := topology.FromJSONFile(topoCore)
			require.NoError(t, err)
			intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
			senderFactory := mock_egress.NewMockSenderFactory(mctrl)
			storage := mock_egress_storage.NewMockEgressDB(mctrl)
			writer := mock_egress.NewMockWriter(mctrl)

			writer.EXPECT().WriterType().AnyTimes().Return(seg.TypeCore)
			writer.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedWrites).DoAndReturn(func(_, _, _, _ interface{}) error {
				return nil
			})
			hashList := make([]string, 0)
			filter := func(intf *ifstate.Interface) bool {
				return intf.TopoInfo().LinkType == topology.Core
			}
			intfMap := make(map[uint32]*ifstate.Interface)
			for _, intf := range intfs.Filtered(filter) {
				intfMap[uint32(intf.TopoInfo().ID)] = intf
			}
			p := egress.Propagator{
				Extender: &egress.DefaultExtender{
					IA:         topo.IA(),
					MTU:        topo.MTU(),
					Signer:     testSigner(t, priv, topo.IA()),
					Intfs:      intfs,
					MAC:        macFactory,
					MaxExpTime: func() uint8 { return beacon.DefaultMaxExpTime },
					StaticInfo: func() *egress.StaticInfoCfg { return nil },
				},
				Core: topo.Core(),
				Pather: &addrutil.Pather{
					NextHopper: beaconing.topoWrap{topo},
				},
				Local:             topo.IA(),
				AllInterfaces:     intfs,
				PropagationFilter: filter,
				PropagationInterfaces: func() []*ifstate.Interface {
					return intfs.Filtered(filter)
				},
				Interfaces:    intfMap,
				SenderFactory: senderFactory,
				Store:         storage,
				Writers:       []beaconing.Writer{writer},
			}
			g := graph.NewDefaultGraph(mctrl)

			bcns := make([]*cppb.EgressBeacon, 0, len(test.beacons))
			for _, bcn := range test.beacons {
				bcns = append(bcns, &cppb.EgressBeacon{
					PathSeg:     seg.PathSegmentToPB(beaconing.testBeacon(g, bcn.desc, true).Segment),
					InIfId:      bcn.inIntfId,
					EgressIntfs: bcn.egressIntf, // The second and third egress interface are not a core or child link and should be ignored.
				})
			}

			senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(
				func(_ context.Context, _ addr.IA, egIfId uint16,
					nextHop *net.UDPAddr) (egress.Sender, error) {

					sender := mock_egress.NewMockSender(mctrl)
					sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(_ context.Context, b *seg.PathSegment) error {
							validateSend(t, b, egIfId, nextHop, pub, topo)
							assert.NotNil(t, b.ASEntries[b.MaxIdx()].Extensions.Irec)

							assert.Equal(t, b.ASEntries[0].Extensions.Irec.AlgorithmHash, b.ASEntries[b.MaxIdx()].Extensions.Irec.AlgorithmHash)
							assert.Equal(t, b.ASEntries[0].Extensions.Irec.AlgorithmId, b.ASEntries[b.MaxIdx()].Extensions.Irec.AlgorithmId)
							return nil
						},
					)
					sender.EXPECT().Close().Times(1)

					return sender, nil
				},
			)

			storage.EXPECT().IsBeaconAlreadyPropagated(gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagationChecks).DoAndReturn(func(arg0 context.Context, arg1 []byte, arg2 *ifstate.Interface) (bool, int, error) {
				return slices.Contains(hashList, strconv.Itoa(int(arg2.TopoInfo().ID))+string(arg1)), 0, nil
			})

			storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(test.expectedPropagatedBeacons).DoAndReturn(func(arg0 context.Context, arg1 []byte, intf *ifstate.Interface, expiry time.Time) error {
				hashList = append(hashList, strconv.Itoa(int(intf.TopoInfo().ID))+string(arg1))
				fmt.Println(hex.EncodeToString(arg1))
				return nil

			})
			//storage.EXPECT().MarkBeaconAsPropagated(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			for i := 0; i < test.runCalls; i++ {
				_, err = p.RequestPropagation(context.Background(), &cppb.PropagationRequest{Beacon: bcns})
				require.NoError(t, err)
			}
		})
	}
}

func validateSend(t *testing.T, b *seg.PathSegment, egIfId uint16, nextHop *net.UDPAddr, pub crypto.PublicKey, topo topology.Topology) {
	// Check the beacon is valid and verifiable.
	assert.NoError(t, b.Validate(seg.ValidateBeacon))
	assert.NoError(t, b.VerifyASEntry(context.Background(),
		segVerifier{pubKey: pub}, b.MaxIdx()))
	// Extract the hop field from the current AS entry to compare.
	hopF := b.ASEntries[b.MaxIdx()].HopEntry.HopField
	// Check the interface matches.
	assert.Equal(t, hopF.ConsEgress, egIfId)
	// Check that the beacon is sent to the correct border router.
	br := net.UDPAddrFromAddrPort(interfaceInfos(topo)[egIfId].InternalAddr)
	assert.Equal(t, br, nextHop)
}

func TestHashing(t *testing.T) {
	beacons := [][]uint16{
		{graph.If_120_X_111_B},
		{graph.If_130_B_120_A, graph.If_120_X_111_B},
		{graph.If_130_B_120_A, graph.If_120_X_111_B},
	}

	mctrl := gomock.NewController(t)
	defer mctrl.Finish()

	// All beacons should give unique hashes;
	g := graph.NewDefaultGraph(mctrl)
	hashes := make([][]byte, 0, len(beacons))
	for _, desc := range beacons {
		hash := egress.HashBeacon(beaconing.testBeacon(g, desc, false).Segment)
		require.NotContains(t, hashes, hash)
		hashes = append(hashes, hash)
	}
	alteredBeacon := beaconing.testBeacon(g, []uint16{graph.If_130_B_120_A, graph.If_120_X_111_B}, false).Segment
	alteredBeacon.Info.SegmentID = uint16(100)
	require.NotContains(t, hashes, egress.HashBeacon(alteredBeacon))
	hashes = append(hashes, egress.HashBeacon(alteredBeacon))

	alteredBeacon.Info.Timestamp = alteredBeacon.Info.Timestamp.Add(50 * time.Minute)
	require.NotContains(t, hashes, egress.HashBeacon(alteredBeacon))
	hashes = append(hashes, egress.HashBeacon(alteredBeacon))

	alteredBeacon.ASEntries[alteredBeacon.MaxIdx()].Extensions.Irec = &irec.Irec{PullBased: true}
	require.NotContains(t, hashes, egress.HashBeacon(alteredBeacon))
	hashes = append(hashes, egress.HashBeacon(alteredBeacon))

	alteredBeacon.ASEntries[alteredBeacon.MaxIdx()].Extensions.Irec.PullBasedTarget = addr.MustIAFrom(1, 50)
	require.NotContains(t, hashes, egress.HashBeacon(alteredBeacon))
	hashes = append(hashes, egress.HashBeacon(alteredBeacon))

	alteredBeacon.ASEntries[alteredBeacon.MaxIdx()].Extensions.Irec.AlgorithmId = 5
	require.NotContains(t, hashes, egress.HashBeacon(alteredBeacon))
}
