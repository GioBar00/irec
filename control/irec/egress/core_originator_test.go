package egress_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"github.com/scionproto/scion/control/beaconing"
	"hash"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/control/irec/egress"
	"github.com/scionproto/scion/control/irec/egress/mock_egress"
	"github.com/scionproto/scion/pkg/addr"
	cryptopb "github.com/scionproto/scion/pkg/proto/crypto"
	"github.com/scionproto/scion/pkg/scrypto"
	"github.com/scionproto/scion/pkg/scrypto/cppki"
	"github.com/scionproto/scion/pkg/scrypto/signed"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/private/topology"
	"github.com/scionproto/scion/private/trust"
)

const (
	topoCore    = "testdata/topology-core.json"
	topoNonCore = "testdata/topology.json"
)

func testSigner(t *testing.T, priv crypto.Signer, ia addr.IA) seg.Signer {
	return trust.Signer{
		PrivateKey: priv,
		Algorithm:  signed.ECDSAWithSHA256,
		IA:         ia,
		TRCID: cppki.TRCID{
			ISD:    ia.ISD(),
			Base:   1,
			Serial: 21,
		},
		SubjectKeyID: []byte("skid"),
		Expiration:   time.Now().Add(time.Hour),
	}
}

var macFactory = func() hash.Hash {
	mac, err := scrypto.InitMac(make([]byte, 16))
	// This can only happen if the library is messed up badly.
	if err != nil {
		panic(err)
	}
	return mac
}

func interfaceInfos(topo topology.Topology) map[uint16]ifstate.InterfaceInfo {
	in := topo.IFInfoMap()
	result := make(map[uint16]ifstate.InterfaceInfo, len(in))
	for id, info := range in {
		result[uint16(id)] = ifstate.InterfaceInfo{
			ID:           uint16(info.ID),
			IA:           info.IA,
			LinkType:     info.LinkType,
			Groups:       info.Groups,
			InternalAddr: netip.MustParseAddrPort(info.InternalAddr.String()),
			RemoteID:     uint16(info.RemoteIfID),
			MTU:          uint16(info.MTU),
		}
	}
	return result
}

func TestOriginatorRun(t *testing.T) {
	topo, err := topology.FromJSONFile(topoCore)
	require.NoError(t, err)

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	pub := priv.Public()
	signer := testSigner(t, priv, topo.IA())
	originationFilter := func(intf *ifstate.Interface) bool {
		topoInfo := intf.TopoInfo()
		if topoInfo.LinkType == topology.Core || topoInfo.LinkType == topology.Child {
			return true
		}
		return false
	}

	t.Run("algoriginator run originates ifid packets on all active interfaces", func(t *testing.T) {
		mctrl := gomock.NewController(t)
		defer mctrl.Finish()
		intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
		senderFactory := mock_egress.NewMockSenderFactory(mctrl)
		bo := egress.BasicOriginator{
			OriginatePerIntfGroup: true,
			Extender: &beaconing.DefaultExtender{
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
		originationAlgorithm := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
		ao := egress.AlgorithmOriginator{
			BasicOriginator: &bo,
			OriginationAlgorithms: []egress.OriginationAlgorithm{{
				ID:   1,
				Hash: originationAlgorithm,
			}},
		}

		require.NoError(t, err)

		senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any()).Times(6).DoAndReturn(
			func(_ context.Context, dstIA addr.IA, egIfId uint16,
				nextHop *net.UDPAddr) (egress.Sender, error) {

				sender := mock_egress.NewMockSender(mctrl)
				sendCalls := 1
				if egIfId == 1113 || egIfId == 2113 || egIfId == 3113 {
					sendCalls = 2
				}
				sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(sendCalls).DoAndReturn(
					func(_ context.Context, b *seg.PathSegment) error {

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
						//// Check that the IREC extension contains the correct origination algorithm
						//algHash := b.ASEntries[b.MaxIdx()].Extensions.Irec.AlgorithmHash
						//assert.Equal(t, algHash, originationAlgorithm)
						//// Check that base orgination does not contain pull based extensions
						//assert.False(t, b.ASEntries[b.MaxIdx()].Extensions.Irec.PullBased)
						return nil
					},
				)
				sender.EXPECT().Close().Times(1)

				return sender, nil
			},
		)

		// Start beacon messages.
		ao.Run(context.Background())
	})
	t.Run("algoriginator run originates correct irec packets", func(t *testing.T) {
		mctrl := gomock.NewController(t)
		defer mctrl.Finish()
		intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
		senderFactory := mock_egress.NewMockSenderFactory(mctrl)
		bo := egress.BasicOriginator{
			OriginatePerIntfGroup: true,
			Extender: &beaconing.DefaultExtender{
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
		originationAlgorithm := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
		ao := egress.AlgorithmOriginator{
			BasicOriginator: &bo,
			OriginationAlgorithms: []egress.OriginationAlgorithm{{
				ID:   1,
				Hash: originationAlgorithm,
			}, {
				ID:   2,
				Hash: originationAlgorithm,
			}},
		}

		require.NoError(t, err)

		senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any()).Times(12).DoAndReturn(
			func(_ context.Context, dstIA addr.IA, egIfId uint16,
				nextHop *net.UDPAddr) (egress.Sender, error) {
				sender := mock_egress.NewMockSender(mctrl)
				sendCalls := 1
				if egIfId == 1113 || egIfId == 2113 || egIfId == 3113 {
					sendCalls = 2
				}
				sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(sendCalls).DoAndReturn(
					func(_ context.Context, b *seg.PathSegment) error {
						// Check that the IREC extension contains the correct origination algorithm
						algHash := b.ASEntries[b.MaxIdx()].Extensions.Irec.AlgorithmHash
						assert.Equal(t, algHash, originationAlgorithm)
						// Check that base orgination does not contain pull based extensions
						assert.False(t, b.ASEntries[b.MaxIdx()].Extensions.Irec.PullBased)
						return nil
					},
				)
				sender.EXPECT().Close().Times(1)

				return sender, nil
			},
		)

		// Start beacon messages.
		ao.Run(context.Background())
	})
	t.Run("algoriginator obeys interface group rules", func(t *testing.T) {
		mctrl := gomock.NewController(t)
		defer mctrl.Finish()
		intfs := ifstate.NewInterfaces(interfaceInfos(topo), ifstate.Config{})
		senderFactory := mock_egress.NewMockSenderFactory(mctrl)
		bo := egress.BasicOriginator{
			OriginatePerIntfGroup: false,
			Extender: &beaconing.DefaultExtender{
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
		originationAlgorithm := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
		ao := egress.AlgorithmOriginator{
			BasicOriginator: &bo,
			OriginationAlgorithms: []egress.OriginationAlgorithm{{
				ID:   1,
				Hash: originationAlgorithm,
			}, {
				ID:   2,
				Hash: originationAlgorithm,
			}},
		}

		require.NoError(t, err)

		senderFactory.EXPECT().NewSender(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any()).Times(12).DoAndReturn(
			func(_ context.Context, dstIA addr.IA, egIfId uint16,
				nextHop *net.UDPAddr) (egress.Sender, error) {
				sender := mock_egress.NewMockSender(mctrl)
				sender.EXPECT().Send(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
					func(_ context.Context, b *seg.PathSegment) error {
						// Check that the IREC extension contains the correct origination algorithm
						algHash := b.ASEntries[b.MaxIdx()].Extensions.Irec.AlgorithmHash
						assert.Equal(t, algHash, originationAlgorithm)
						// Check that base orgination does not contain pull based extensions
						assert.False(t, b.ASEntries[b.MaxIdx()].Extensions.Irec.PullBased)
						assert.Equal(t, uint16(0), b.ASEntries[b.MaxIdx()].Extensions.Irec.InterfaceGroup)
						return nil
					},
				)
				sender.EXPECT().Close().Times(1)

				return sender, nil
			},
		)

		// Start beacon messages.
		ao.Run(context.Background())
	})
}

type segVerifier struct {
	pubKey crypto.PublicKey
}

func (v segVerifier) Verify(_ context.Context, signedMsg *cryptopb.SignedMessage,
	associatedData ...[]byte) (*signed.Message, error) {

	return signed.Verify(signedMsg, v.pubKey, associatedData...)
}
