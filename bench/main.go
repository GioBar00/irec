package main

import (
	"context"
	"fmt"
	flatbuffers "github.com/google/flatbuffers/go"
	ingressmockstorage "github.com/scionproto/scion/bench/mock/ingress/sqlite"
	"github.com/scionproto/scion/control/beacon"
	"github.com/scionproto/scion/control/irec/ingress"
	storage2 "github.com/scionproto/scion/control/irec/ingress/storage"
	"github.com/scionproto/scion/pkg/addr"
	libgrpc "github.com/scionproto/scion/pkg/grpc"
	IREC "github.com/scionproto/scion/pkg/irec/includes/flatbuffers-go/irec"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/common"
	"github.com/scionproto/scion/pkg/private/serrors"
	"github.com/scionproto/scion/pkg/private/xtest/graph"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/pkg/segment/extensions/staticinfo"
	"github.com/scionproto/scion/pkg/slayers/path"
	"github.com/scionproto/scion/private/app"
	"github.com/scionproto/scion/rac/config"
	"github.com/scionproto/scion/rac/env/ebpf"
	"github.com/scionproto/scion/rac/env/native"
	"github.com/scionproto/scion/rac/env/wasm"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const defaultNewSenderTimeout = 50000 * time.Second

const maximumSelectableBeacons = 30

var (
	IA311 = addr.MustIAFrom(1, 0xff0000000311)
	IA330 = addr.MustIAFrom(1, 0xff0000000330)
	IA331 = addr.MustIAFrom(1, 0xff0000000331)
	IA332 = addr.MustIAFrom(1, 0xff0000000332)
	IA333 = addr.MustIAFrom(1, 0xff0000000333)

	IA334 = addr.MustIAFrom(1, 0xff0000000334)
	IA335 = addr.MustIAFrom(1, 0xff0000000335)
	IA336 = addr.MustIAFrom(1, 0xff0000000336)
	IA337 = addr.MustIAFrom(1, 0xff0000000337)
	IA338 = addr.MustIAFrom(1, 0xff0000000338)
	IA339 = addr.MustIAFrom(1, 0xff0000000339)
	IA340 = addr.MustIAFrom(1, 0xff0000000340)
	IA341 = addr.MustIAFrom(1, 0xff0000000341)

	Info1 = []IfInfo{
		{
			IA:     IA330,
			Next:   IA331,
			Egress: 5,
		},
		{
			IA:      IA331,
			Next:    IA332,
			Ingress: 2,
			Egress:  3,
			Peers:   []PeerEntry{{IA: IA311, Ingress: 6}},
		},
		{
			IA:      IA332,
			Next:    IA333,
			Ingress: 1,
			Egress:  7,
		},
	}
	Info2 = []IfInfo{
		{
			IA:     IA330,
			Next:   IA334,
			Egress: 4,
		},
		{
			IA:     IA334,
			Next:   IA335,
			Egress: 5,
		},
		{
			IA:     IA335,
			Next:   IA336,
			Egress: 5,
		},

		{
			IA:     IA336,
			Next:   IA337,
			Egress: 5,
		},
		{
			IA:     IA337,
			Next:   IA338,
			Egress: 5,
		},
		{
			IA:     IA338,
			Next:   IA339,
			Egress: 5,
		},
		{
			IA:     IA339,
			Next:   IA340,
			Egress: 5,
		},
		{
			IA:     IA340,
			Next:   IA341,
			Egress: 5,
		},
		{
			IA:      IA341,
			Next:    IA332,
			Ingress: 2,
			Egress:  3,
			Peers:   []PeerEntry{{IA: IA311, Ingress: 6}},
		},
		{
			IA:      IA332,
			Next:    IA333,
			Ingress: 1,
			Egress:  7,
		},
	}

	Info3 = []IfInfo{
		{
			IA:     IA330,
			Next:   IA334,
			Egress: 5,
		},
		{
			IA:     IA334,
			Next:   IA335,
			Egress: 5,
		},
		{
			IA:     IA335,
			Next:   IA336,
			Egress: 5,
		},

		{
			IA:     IA336,
			Next:   IA337,
			Egress: 5,
		},
		{
			IA:     IA337,
			Next:   IA338,
			Egress: 5,
		},
		{
			IA:     IA338,
			Next:   IA339,
			Egress: 5,
		},
		{
			IA:      IA339,
			Next:    IA332,
			Ingress: 2,
			Egress:  3,
			Peers:   []PeerEntry{{IA: IA311, Ingress: 6}},
		},
		{
			IA:      IA332,
			Next:    IA333,
			Ingress: 1,
			Egress:  7,
		},
	}

	Info4 = []IfInfo{
		{
			IA:     IA330,
			Next:   IA333,
			Egress: 8,
		},
		{
			IA:      IA332,
			Next:    IA333,
			Ingress: 1,
			Egress:  7,
		},
	}

	Info5 = []IfInfo{
		{
			IA:     IA330,
			Next:   IA334,
			Egress: 5,
		},
		{
			IA:     IA334,
			Next:   IA335,
			Egress: 5,
		},
		{
			IA:     IA335,
			Next:   IA336,
			Egress: 5,
		},

		{
			IA:     IA336,
			Next:   IA337,
			Egress: 5,
		},
		{
			IA:     IA337,
			Next:   IA338,
			Egress: 5,
		},
		{
			IA:     IA338,
			Next:   IA339,
			Egress: 5,
		},
		{
			IA:     IA339,
			Next:   IA340,
			Egress: 5,
		},
		{
			IA:     IA340,
			Next:   IA338,
			Egress: 5,
		},
		{
			IA:     IA338,
			Next:   IA339,
			Egress: 5,
		},
		{
			IA:     IA339,
			Next:   IA340,
			Egress: 5,
		},
		{
			IA:     IA340,
			Next:   IA341,
			Egress: 5,
		},
		{
			IA:      IA341,
			Next:    IA332,
			Ingress: 2,
			Egress:  3,
			Peers:   []PeerEntry{{IA: IA311, Ingress: 6}},
		},
		{
			IA:      IA332,
			Next:    IA333,
			Ingress: 1,
			Egress:  7,
		},
	}
)

type PeerEntry struct {
	IA      addr.IA
	Ingress common.IFIDType
}

type IfInfo struct {
	IA      addr.IA
	Next    addr.IA
	Ingress common.IFIDType
	Egress  common.IFIDType
	Peers   []PeerEntry
}

func MockBeacon(ases []IfInfo, id uint16, inIfId uint16, infoTS int64) (beacon.Beacon, *cppb.IRECPathSegmentExcerpt, []byte, []byte) {

	entries := make([]seg.ASEntry, len(ases))
	for i, as := range ases {
		var mtu int
		if i != 0 {
			mtu = 1500
		}
		var peers []seg.PeerEntry
		for _, peer := range as.Peers {
			peers = append(peers, seg.PeerEntry{
				Peer:          peer.IA,
				PeerInterface: 1337,
				PeerMTU:       1500,
				HopField: seg.HopField{
					ExpTime:     63,
					ConsIngress: uint16(peer.Ingress),
					ConsEgress:  uint16(as.Egress),
					MAC:         [path.MacLen]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
			})
		}
		entries[i] = seg.ASEntry{
			Local: as.IA,
			Next:  as.Next,
			MTU:   1500,
			HopEntry: seg.HopEntry{
				IngressMTU: mtu,
				HopField: seg.HopField{
					ExpTime:     63,
					ConsIngress: uint16(as.Ingress),
					ConsEgress:  uint16(as.Egress),
					MAC:         [path.MacLen]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				},
			},
			PeerEntries: peers,
			Extensions: seg.Extensions{
				HiddenPath: seg.HiddenPathExtension{},
				StaticInfo: &staticinfo.Extension{
					Latency: staticinfo.LatencyInfo{
						Intra: nil,
						Inter: nil,
					},
					Bandwidth:    staticinfo.BandwidthInfo{},
					Geo:          nil,
					LinkType:     nil,
					InternalHops: nil,
					Note:         "",
				},
				Digests: nil,
				Irec:    nil,
			},
		}
	}

	// XXX(roosd): deterministic beacon needed.
	pseg, err := seg.CreateSegment(time.Unix(int64(infoTS), 0), id)
	if err != nil {
		//log.Error("err", "err", err)
		return beacon.Beacon{}, &cppb.IRECPathSegmentExcerpt{}, nil, []byte{}
	}

	asEntries := make([]*cppb.IRECASEntryExcerpt, 0, len(entries))
	for _, entry := range entries {
		signer := graph.NewSigner()
		// for testing purposes set the signer timestamp equal to infoTS
		signer.Timestamp = time.Unix(int64(infoTS), 0)
		err := pseg.AddASEntry(context.Background(), entry, signer)
		if err != nil {
			return beacon.Beacon{}, &cppb.IRECPathSegmentExcerpt{}, nil, []byte{}
		}
	}
	// above loop adds to the pseg asentries, now transform them into irec as entries.
	// bit hacky

	for _, entry := range pseg.ASEntries {
		asEntries = append(asEntries, &cppb.IRECASEntryExcerpt{
			//Signed:     entry.Signed,
			SignedBody: entry.SignedBody,
			Unsigned:   seg.UnsignedExtensionsToPB(entry.UnsignedExtensions),
		})
	}
	//todo(jvb); signed body komt inet in db door mockbeacon
	b := beacon.Beacon{Segment: pseg, InIfId: inIfId}
	flatbufferBeacon, err := beacon.PackBeaconFB(&b)

	if err != nil {
		return beacon.Beacon{}, &cppb.IRECPathSegmentExcerpt{}, nil, []byte{}
	}
	return b, &cppb.IRECPathSegmentExcerpt{
		SegmentInfo: pseg.Info.Raw,
		AsEntries:   asEntries,
	}, pseg.ID(), flatbufferBeacon
}

var globalCfg Config

func packBeaconsFlatbuffer(fbs [][]byte, PropagationInterfaces []uint32) []byte {
	builder := flatbuffers.NewBuilder(0)
	fbOffsetArr := make([]flatbuffers.UOffsetT, len(fbs))

	for j, buf := range fbs {
		data := builder.CreateByteVector(buf)

		IREC.BeaconBytesWrapperStart(builder)
		IREC.BeaconBytesWrapperAddB(builder, data)
		fbOffsetArr[j] = IREC.BeaconBytesWrapperEnd(builder)
	}

	IREC.ExecutionStartBeaconsVector(builder, len(fbOffsetArr))
	for _, region := range fbOffsetArr {
		builder.PrependUOffsetT(region)
	}
	beacons := builder.EndVector(len(fbOffsetArr))

	IREC.ExecutionStartIntfsVector(builder, len(PropagationInterfaces))
	for _, intf := range PropagationInterfaces {
		IREC.CreateInterface(builder, intf)
	}
	intfs := builder.EndVector(len(PropagationInterfaces))

	IREC.ExecutionStart(builder)
	IREC.ExecutionAddBeacons(builder, beacons)
	IREC.ExecutionAddIntfs(builder, intfs)
	execution := IREC.ExecutionEnd(builder)

	builder.Finish(execution)

	finishedBuf := builder.FinishedBytes()
	return finishedBuf
}

type DummyWriter struct {
}

func (d DummyWriter) WriteBeacons(ctx context.Context, beacons []*cppb.EgressBeacon) error {
	return nil
}

func main() {
	if err := log.Setup(log.Config{
		Console: log.ConsoleConfig{Format: "human", Level: "debug"},
	}); err != nil {
		fmt.Println(err)
		return
	}
	defer log.Flush()
	defer log.HandlePanic()
	if os.Args[3] == "rawthr" {
		propIntfs := []uint32{0, 1, 2}
		hash := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
		amountOfBeacons, _ := strconv.Atoi(os.Args[1])
		s1 := rand.NewSource(42)
		r1 := rand.New(s1)
		var bcns []*cppb.IRECBeacon
		var bcnsUnopt []*cppb.IRECBeaconUnopt
		var allFbs [][]byte
		var rowIds []int64
		var egressBcns []*cppb.EgressBeacon
		for i := 0; i < amountOfBeacons; i++ {
			var bcn beacon.Beacon
			var excerpt *cppb.IRECPathSegmentExcerpt
			var fbs []byte
			switch r1.Intn(5) {
			case 0:
				bcn, excerpt, _, fbs = MockBeacon(Info1, uint16(i), 0, time.Now().Unix())
				break
			case 1:
				bcn, excerpt, _, fbs = MockBeacon(Info2, uint16(i), 0, time.Now().Unix())
				break
			case 2:
				bcn, excerpt, _, fbs = MockBeacon(Info3, uint16(i), 0, time.Now().Unix())
				break
			case 3:
				bcn, excerpt, _, fbs = MockBeacon(Info4, uint16(i), 0, time.Now().Unix())
				break
			case 4:
				bcn, excerpt, _, fbs = MockBeacon(Info5, uint16(i), 0, time.Now().Unix())
				break
			}
			allFbs = append(allFbs, fbs)
			rowIds = append(rowIds, int64(i))

			//unopt := &cppb.IRECPathSegment{
			//	SegmentInfo: bcn.Segment.Info.Raw,
			//	AsEntries:   make([]*cppb.IRECASEntry, 0, len(bcn.Segment.ASEntries)),
			//}
			//
			//for _, entry := range bcn.Segment.ASEntries {
			//	unopt.AsEntries = append(unopt.AsEntries, &cppb.IRECASEntry{
			//		Signed:     entry.Signed,
			//		SignedBody: entry.SignedBody,
			//		Unsigned:   seg.UnsignedExtensionsToPB(entry.UnsignedExtensions),
			//	})
			//}
			bcns = append(bcns, &cppb.IRECBeacon{
				PathSeg: excerpt,
				InIfId:  uint32(bcn.InIfId),
				Id:      int64(i),
			})
			bcnsUnopt = append(bcnsUnopt, &cppb.IRECBeaconUnopt{
				PathSeg: seg.PathSegmentToPB(bcn.Segment),
				InIfId:  uint32(bcn.InIfId),
				Id:      int64(i),
			})
			egressBcns = append(egressBcns, &cppb.EgressBeacon{
				PathSeg:         seg.PathSegmentToPB(bcn.Segment),
				InIfId:          uint32(bcn.InIfId),
				EgressIntfs:     nil,
				PullbasedTarget: 0,
			})
		}
		fileBinary, err := os.ReadFile(FILE)
		if err != nil {
			log.Error("An error occurred while loading algorithm "+FILE+"; skipping.", "err", err)
			return
		}
		var job *cppb.RACJob
		if MODE == "UBPF" {
			job = &cppb.RACJob{
				AlgorithmHash: hash,
				Flatbuffer:    packBeaconsFlatbuffer(allFbs, propIntfs),
				BeaconCount:   uint32(len(allFbs)),
				RowIds:        rowIds,
			}
		} else if MODE == "WA" {
			job = &cppb.RACJob{
				Beacons:       bcns,
				AlgorithmHash: hash,
				PropIntfs:     propIntfs,
				BeaconCount:   uint32(len(bcns)),
				RowIds:        rowIds,
			}
		} else if MODE == "WAOPT" || MODE == "NATIVE" {
			job = &cppb.RACJob{
				BeaconsUnopt:  bcnsUnopt,
				AlgorithmHash: hash,
				PropIntfs:     propIntfs,
				BeaconCount:   uint32(len(bcns)),
				RowIds:        rowIds,
			}
		}

		dialer := &libgrpc.TCPDialer{
			SvcResolver: func(dst addr.HostSVC) []resolver.Address {
				if base := dst.Base(); base != addr.SvcCS {
					panic("Unsupported address type, implementation error?")
				}
				targets := []resolver.Address{}
				targets = append(targets, resolver.Address{Addr: "127.0.0.1:33333"})
				return targets
			},
		}
		conn, err := dialer.DialLimit(context.Background(), addr.SvcCS, 100)
		if err != nil {
			log.Error("error occurred", "err", err)
			return
		}
		defer conn.Close()
		is := &IngressDummy{
			Job:           job,
			Algorithm:     &cppb.AlgorithmResponse{Code: fileBinary},
			EgressBeacons: egressBcns,
			Dialer:        dialer,
			Conn:          conn,
		}
		env := BenchEnv{}
		t := atomic.Uint64{}
		env.RegisterServers(&SimpleEgressCounter{Results: &t}, is, false)
		time.Sleep(5 * time.Second)
		amtIters, _ := strconv.Atoi(os.Args[4])
		for i := 0; i < amtIters; i++ {
			t.Store(0)
			start := time.Now()
			time.Sleep(10 * time.Second)
			count := t.Load()
			duration := time.Since(start)
			fmt.Printf("%d, %d, %f pcb/s\n", duration.Nanoseconds(), count, float64(count*uint64(amountOfBeacons))/duration.Seconds())
		}
		return
	}

	if os.Args[3] == "max" {
		propIntfs := []uint32{0, 1, 2}
		hash := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
		amountOfBeacons, _ := strconv.Atoi(os.Args[1])
		s1 := rand.NewSource(42)
		r1 := rand.New(s1)
		var bcns []*cppb.IRECBeacon
		var bcnsUnopt []*cppb.IRECBeaconUnopt
		var allFbs [][]byte
		var rowIds []int64
		var egressBcns []*cppb.EgressBeacon
		for i := 0; i < amountOfBeacons; i++ {
			var bcn beacon.Beacon
			var excerpt *cppb.IRECPathSegmentExcerpt
			var fbs []byte
			switch r1.Intn(5) {
			case 0:
				bcn, excerpt, _, fbs = MockBeacon(Info1, uint16(i), 0, time.Now().Unix())
				break
			case 1:
				bcn, excerpt, _, fbs = MockBeacon(Info2, uint16(i), 0, time.Now().Unix())
				break
			case 2:
				bcn, excerpt, _, fbs = MockBeacon(Info3, uint16(i), 0, time.Now().Unix())
				break
			case 3:
				bcn, excerpt, _, fbs = MockBeacon(Info4, uint16(i), 0, time.Now().Unix())
				break
			case 4:
				bcn, excerpt, _, fbs = MockBeacon(Info5, uint16(i), 0, time.Now().Unix())
				break
			}
			allFbs = append(allFbs, fbs)
			rowIds = append(rowIds, int64(i))
			//unopt := &cppb.IRECPathSegment{
			//	SegmentInfo: bcn.Segment.Info.Raw,
			//	AsEntries:   make([]*cppb.IRECASEntry, 0, len(bcn.Segment.ASEntries)),
			//}
			////
			//for _, entry := range bcn.Segment.ASEntries {
			//	unopt.AsEntries = append(unopt.AsEntries, &cppb.IRECASEntry{
			//		Signed:     entry.Signed,
			//		SignedBody: entry.SignedBody,
			//		Unsigned:   seg.UnsignedExtensionsToPB(entry.UnsignedExtensions),
			//	})
			//}
			bcns = append(bcns, &cppb.IRECBeacon{
				PathSeg: excerpt,
				InIfId:  uint32(bcn.InIfId),
				Id:      int64(i),
			})
			bcnsUnopt = append(bcnsUnopt, &cppb.IRECBeaconUnopt{
				PathSeg: seg.PathSegmentToPB(bcn.Segment),
				InIfId:  uint32(bcn.InIfId),
				Id:      int64(i),
			})
			egressBcns = append(egressBcns, &cppb.EgressBeacon{
				PathSeg:         seg.PathSegmentToPB(bcn.Segment),
				InIfId:          uint32(bcn.InIfId),
				EgressIntfs:     nil,
				PullbasedTarget: 0,
			})
		}

		fileBinary, err := os.ReadFile(FILE)
		if err != nil {
			log.Error("An error occurred while loading algorithm "+FILE+"; skipping.", "err", err)
			return
		}

		if MODE == "UBPF" {
			env := &ebpf.EbpfEnv{Writer: &DummyWriter{}, Static: os.Args[4] == "static", JIT: os.Args[5] == "j"}
			ctx := context.Background()
			job := &cppb.RACJob{
				AlgorithmHash: hash,
				Flatbuffer:    packBeaconsFlatbuffer(allFbs, propIntfs),
				BeaconCount:   uint32(len(allFbs)),
				RowIds:        rowIds,
			}

			if os.Args[4] == "static" {
				env.InitStaticAlgo(config.RACAlgorithm{
					FilePath: FILE,
				})
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteStatic(ctx, job, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteStatic(ctx, job, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			} else {
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			}

			env.ExecuteDynamic(ctx, job, fileBinary, 0)
		} else if MODE == "WA" || MODE == "WAOPT" {

			env := &wasm.WasmEnv{Writer: &DummyWriter{}}
			env.Initialize()
			ctx := context.Background()
			var job *cppb.RACJob
			if MODE == "WA" {
				job = &cppb.RACJob{
					Beacons:       bcns,
					AlgorithmHash: hash,
					PropIntfs:     propIntfs,
					BeaconCount:   uint32(len(bcns)),
					RowIds:        rowIds,
				}
			} else if MODE == "WAOPT" {
				job = &cppb.RACJob{
					BeaconsUnopt:  bcnsUnopt,
					AlgorithmHash: hash,
					PropIntfs:     propIntfs,
					BeaconCount:   uint32(len(bcns)),
					RowIds:        rowIds,
				}
			}

			if os.Args[4] == "static" {
				env.InitStaticAlgo(config.RACAlgorithm{
					FilePath: FILE,
				})
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteStatic(ctx, job, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteStatic(ctx, job, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			} else {
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			}
		} else if MODE == "NATIVE" {

			env := &native.NativeEnv{Writer: &DummyWriter{}}
			env.Initialize()
			ctx := context.Background()
			job := &cppb.RACJob{
				BeaconsUnopt:  bcnsUnopt,
				AlgorithmHash: hash,
				PropIntfs:     propIntfs,
				BeaconCount:   uint32(len(bcns)),
				RowIds:        rowIds,
			}

			if os.Args[4] == "static" {
				env.InitStaticAlgo(config.RACAlgorithm{
					FilePath: FILE,
				})
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteStatic(ctx, job, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteStatic(ctx, job, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			} else {
				var i int
				totalTimeS := time.Now()
				for time.Since(totalTimeS) < 10*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
				}

				totalTimeS = time.Now()
				for time.Since(totalTimeS) < 60*time.Second {
					env.ExecuteDynamic(ctx, job, fileBinary, 0)
					i++
				}
				totalTime := time.Since(totalTimeS)
				//pcbS := int64(i*amountOfBeacons) / totalTime.Nanoseconds()
				fmt.Printf("Result: %d, %d, %d, %f PCB/s\n", i, amountOfBeacons, totalTime.Nanoseconds(), float64(i*amountOfBeacons)/totalTime.Seconds())
			}

			env.ExecuteDynamic(ctx, job, fileBinary, 0)
		}
	}

	amountOfBeacons, _ := strconv.Atoi(os.Args[1])
	//beacons := make([]beacon.Beacon, amountOfBeacons)
	//irecbeacons := make([]*cppb.IRECBeacon, amountOfBeacons)
	//flatbuffers := make([][]byte, amountOfBeacons)
	//rowIds := make([]int64, amountOfBeacons)
	os.Remove("bench_conf/ingressdb.db")
	igdb, err := ingressmockstorage.New("bench_conf/ingressdb.db", addr.MustIAFrom(1, 0x001))
	defer igdb.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	igdb.SetMaxOpenConns(5000)
	igdb.SetMaxIdleConns(5000)
	s1 := rand.NewSource(42)
	r1 := rand.New(s1)
	for i := 0; i < amountOfBeacons; i++ {
		var bcn beacon.Beacon
		switch r1.Intn(5) {
		case 0:
			bcn, _, _, _ = MockBeacon(Info1, uint16(i), 0, time.Now().Unix())
			break
		case 1:
			bcn, _, _, _ = MockBeacon(Info2, uint16(i), 0, time.Now().Unix())
			break
		case 2:
			bcn, _, _, _ = MockBeacon(Info3, uint16(i), 0, time.Now().Unix())
			break
		case 3:
			bcn, _, _, _ = MockBeacon(Info4, uint16(i), 0, time.Now().Unix())
			break
		case 4:
			bcn, _, _, _ = MockBeacon(Info5, uint16(i), 0, time.Now().Unix())
			break
		}
		_, err := igdb.InsertBeacon(context.Background(), bcn, beacon.UsageProp)
		if err != nil {
			fmt.Println(err)
			return
		}
		fileBinary, err := os.ReadFile(FILE)
		if err != nil {
			log.Error("An error occurred while loading algorithm "+FILE+"; skipping.", "err", err)
			return
		}
		err = igdb.AddAlgorithm(context.Background(), []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, fileBinary)

	}

	if os.Args[3] == "latency" || os.Args[3] == "bw" {

		var policies beacon.Policies
		db, err := storage2.NewIngressDB(policies, igdb)
		if err != nil {
			fmt.Println(err)
			return
		}
		is := ingress.IngressServer{
			IncomingHandler:       nil,
			IngressDB:             db,
			PropagationInterfaces: []uint32{0, 1, 2},
			Dialer: &libgrpc.TCPDialer{
				SvcResolver: func(dst addr.HostSVC) []resolver.Address {
					if base := dst.Base(); base != addr.SvcCS {
						panic("Unsupported address type, implementation error?")
					}
					targets := []resolver.Address{}
					targets = append(targets, resolver.Address{Addr: "127.0.0.1:33333"})
					return targets
				},
			},
		}
		//is.LoadAlgorithms(context.Background(), []config.AlgorithmInfo{{
		//	Originate: false,
		//	ID:        0,
		//	File:      "algorithms/deadbeef2.o",
		//	Fallback:  false,
		//}})
		env := BenchEnv{}
		if os.Args[3] == "bw" {
			env.RegisterServers(&EgressLogger{}, &is, true)
		} else {
			env.RegisterServers(&EgressLogger{}, &is, false)
		}
		g, errCtx := errgroup.WithContext(context.Background())
		g.Go(func() error {
			defer log.HandlePanic()
			<-errCtx.Done()
			return nil
		})

		g.Wait()
	} else if os.Args[3] == "throughput" {
		var policies beacon.Policies
		db, err := storage2.NewIngressDB(policies, igdb)
		if err != nil {
			fmt.Println(err)
			return
		}
		is := ingress.IngressServer{
			IncomingHandler:       nil,
			IngressDB:             db,
			PropagationInterfaces: []uint32{0, 1, 2},
			Dialer: &libgrpc.TCPDialer{
				SvcResolver: func(dst addr.HostSVC) []resolver.Address {
					if base := dst.Base(); base != addr.SvcCS {
						panic("Unsupported address type, implementation error?")
					}
					targets := []resolver.Address{}
					targets = append(targets, resolver.Address{Addr: "127.0.0.1:33333"})
					return targets
				},
			},
		}
		env := BenchEnv{}
		t := atomic.Uint64{}
		env.RegisterServers(&SimpleEgressCounter{Results: &t}, &is, false)
		time.Sleep(5 * time.Second)
		amtIters, _ := strconv.Atoi(os.Args[4])
		for i := 0; i < amtIters; i++ {
			t.Store(0)
			start := time.Now()
			time.Sleep(10 * time.Second)
			count := t.Load()
			duration := time.Since(start)
			fmt.Printf("%d, %d, %f pcb/s\n", duration.Nanoseconds(), count, float64(count*uint64(amountOfBeacons))/duration.Seconds())
		}
	}

	//
	////t := make(map[uint32]*int64)
	//if os.Args[3] == "stress" {
	//	// Used in stress tests; measure the PCB/s of incoming requests.
	//	t := atomic.Uint64{}
	//	env.RegisterServers(&SimpleEgressCounter{Results: &t}, &IngressCounter{env: &env, Results: &t})
	//	time.Sleep(5 * time.Second)
	//	amtIters, _ := strconv.Atoi(os.Args[4])
	//	for i := 0; i < amtIters; i++ {
	//		t.Store(0)
	//		start := time.Now()
	//		time.Sleep(10 * time.Second)
	//		count := t.Load()
	//		duration := time.Since(start)
	//		fmt.Printf("%d, %d, %f pcb/s\n", duration.Nanoseconds(), count, float64(count*uint64(amountOfBeacons))/duration.Seconds())
	//	}
	//} else if os.Args[3] == "rac" {
	//	// Spawns a server, which just serves requests and logs the time at which the egress prop request came in
	//	// Use this to measure on the RACs itself
	//	env.RegisterServers(&EgressLogger{}, &IngressDummy{env: &env})
	//	g, errCtx := errgroup.WithContext(context.Background())
	//	g.Go(func() error {
	//		defer log.HandlePanic()
	//		<-errCtx.Done()
	//		return nil
	//	})
	//
	//	g.Wait()
	//} else if os.Args[3] == "scion" {
	//	// Measure SCION Latency for the whole algorithm.
	//	time.Sleep(5 * time.Second)
	//	amtIters, _ := strconv.Atoi(os.Args[4])
	//	//env.RegisterIngressServer(&EgressLogger{})
	//	for i := 0; i < amtIters; i++ {
	//		env.BenchSCION()
	//	}
	//} else if os.Args[3] == "scionthr" {
	//	// Measure SCION throughput for the algorithm over 10 seconds.
	//	amtIters, _ := strconv.Atoi(os.Args[4])
	//	for rep := 0; rep < amtIters; rep += 1 {
	//		i := 0
	//		start := time.Now()
	//		for time.Since(start) < 10*time.Second {
	//			i += 1
	//			baseAlgo{}.SelectBeacons(env.beacons, 20)
	//
	//		}
	//		duration := time.Since(start)
	//		fmt.Printf("%d, %d, %f pcb/s\n", duration.Nanoseconds(), i, float64(uint64(i)*uint64(amountOfBeacons))/duration.Seconds())
	//
	//	}
	//}

	//testing.Benchmark(BenchmarkRac)

}

type BenchEnv struct {
	beacons            []beacon.Beacon
	irecbeacons        []*cppb.IRECBeacon
	tcpServer          *grpc.Server
	flatbuffers        [][]byte
	flatbuffers_algoid [][]byte
	rowIds             []int64
}

type EgressLogger struct {
}

func (e *EgressLogger) PullBasedCallback(ctx context.Context, incomingBeacon *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (e *EgressLogger) RequestPullBasedOrigination(ctx context.Context, request *cppb.PullPathsRequest) (*cppb.PropagationRequestResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (e *EgressLogger) RequestPropagation(ctx context.Context, request *cppb.PropagationRequest) (*cppb.PropagationRequestResponse, error) {
	//completed := time.Now().UnixNano()
	//fmt.Println("\t[RAC]=e", request.Beacon[0].EgressIntfs[0], ",", completed)
	return &cppb.PropagationRequestResponse{}, nil
}

type SimpleEgressCounter struct {
	Results *atomic.Uint64
}

func (e *SimpleEgressCounter) PullBasedCallback(ctx context.Context, incomingBeacon *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (e *SimpleEgressCounter) RequestPullBasedOrigination(ctx context.Context, request *cppb.PullPathsRequest) (*cppb.PropagationRequestResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (e *SimpleEgressCounter) RequestPropagation(ctx context.Context, request *cppb.PropagationRequest) (*cppb.PropagationRequestResponse, error) {
	e.Results.Add(1)
	return &cppb.PropagationRequestResponse{}, nil
}

type EgressCounter struct {
	Results map[uint32]*int64
}

func (e *EgressCounter) RequestPropagation(ctx context.Context, request *cppb.PropagationRequest) (*cppb.PropagationRequestResponse, error) {
	atomic.AddInt64(e.Results[request.Beacon[0].EgressIntfs[0]], 1)
	return &cppb.PropagationRequestResponse{}, nil
}

type IngressCounter struct {
	env     *BenchEnv
	Results *atomic.Uint64
}

func (i *IngressCounter) GetBeacons(ctx context.Context, query *cppb.BeaconQuery) (*cppb.RACJob, error) {
	//TODO implement me
	panic("implement me")
}

func (i *IngressCounter) GetAlgorithm(ctx context.Context, request *cppb.AlgorithmRequest) (*cppb.AlgorithmResponse, error) {
	return nil, nil
}

func (i *IngressCounter) GetJob(ctx context.Context, request *cppb.RACBeaconRequest) (*cppb.RACJob, error) {
	return &cppb.RACJob{
		Beacons:       i.env.irecbeacons,
		Flatbuffer:    i.env.flatbuffers_algoid[request.AlgorithmID],
		AlgorithmHash: []byte{0x00, 0xde, 0xad, 0xbe, 0xef},
		BeaconCount:   uint32(len(i.env.irecbeacons)),
		RowIds:        i.env.rowIds,
	}, nil
}

func (i *IngressCounter) Handle(ctx context.Context, req *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	return &cppb.IncomingBeaconResponse{}, nil
}

func (i *IngressCounter) BeaconSources(ctx context.Context, request *cppb.RACBeaconSourcesRequest) (*cppb.RACBeaconSources, error) {
	return &cppb.RACBeaconSources{
		Sources: []*cppb.RACBeaconSource{&cppb.RACBeaconSource{
			AlgorithmHash:   []byte{0x00, 0xde, 0xad, 0xbe, 0xef},
			AlgorithmID:     0,
			OriginAS:        0,
			OriginIntfGroup: 0,
		}},
	}, nil
}
func (i *IngressCounter) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	//err := i.IngressDB.MarkBeacons(ctx, req.RowIDs)
	//if err != nil {
	//	return &cppb.JobCompleteResponse{}, err
	//}
	i.Results.Add(1)
	return &cppb.JobCompleteResponse{}, nil
}

func (i *IngressCounter) RequestBeacons(ctx context.Context, req *cppb.RACBeaconRequest) (*cppb.RACBeaconResponse, error) {
	return &cppb.RACBeaconResponse{}, nil
}

type IngressDummy struct {
	Job           *cppb.RACJob
	Algorithm     *cppb.AlgorithmResponse
	EgressBeacons []*cppb.EgressBeacon
	Dialer        *libgrpc.TCPDialer
	Conn          *grpc.ClientConn
}

func (i *IngressDummy) GetBeacons(ctx context.Context, query *cppb.BeaconQuery) (*cppb.RACJob, error) {
	return i.Job, nil
}

func (i *IngressDummy) GetAlgorithm(ctx context.Context, request *cppb.AlgorithmRequest) (*cppb.AlgorithmResponse, error) {
	return i.Algorithm, nil
}

func (i *IngressDummy) GetJob(ctx context.Context, request *cppb.RACBeaconRequest) (*cppb.RACJob, error) {
	return i.Job, nil
}

func (i *IngressDummy) Handle(ctx context.Context, req *cppb.IncomingBeacon) (*cppb.IncomingBeaconResponse, error) {
	return &cppb.IncomingBeaconResponse{}, nil
}

func (i *IngressDummy) BeaconSources(ctx context.Context, request *cppb.RACBeaconSourcesRequest) (*cppb.RACBeaconSources, error) {
	return &cppb.RACBeaconSources{}, nil
}
func (i *IngressDummy) JobComplete(ctx context.Context, req *cppb.JobCompleteNotify) (*cppb.JobCompleteResponse, error) {
	go func() {
		defer log.HandlePanic()
		req := req
		senderCtx, cancelF := context.WithTimeout(context.Background(), defaultNewSenderTimeout)
		defer cancelF()
		beacons := make([]*cppb.EgressBeacon, 0)
		for x, selected := range req.Selection {
			bcn := i.EgressBeacons[x]
			bcn.EgressIntfs = selected.EgressIntfs
			beacons = append(beacons, bcn)
		}

		client := cppb.NewEgressIntraServiceClient(i.Conn)
		_, err := client.RequestPropagation(senderCtx, &cppb.PropagationRequest{
			Beacon: beacons,
		}, libgrpc.RetryProfile...)
		if err != nil {
			log.Error("error occurred", "err", err)
			return
		}
	}()

	return &cppb.JobCompleteResponse{}, nil
}

func (i *IngressDummy) RequestBeacons(ctx context.Context, req *cppb.RACBeaconRequest) (*cppb.RACBeaconResponse, error) {
	return &cppb.RACBeaconResponse{}, nil
}

type recorderCtxKey struct {
}

type recorder struct {
	size int
	name string
}

type statsHandler struct{}

func (sl *statsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (sl *statsHandler) HandleConn(ctx context.Context, connStats stats.ConnStats) {

}

func (sl *statsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	fmt.Println(info)
	return context.WithValue(ctx, recorderCtxKey{}, &recorder{name: info.FullMethodName})
}

func (h *statsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {

	switch stat := rpcStats.(type) {
	case *stats.InPayload:
		r, _ := ctx.Value(recorderCtxKey{}).(*recorder)
		r.size += stat.WireLength
		fmt.Println("Using bw: ", r.name, r.size)
		//case *stats.End:
		//	durationMs := stat.EndTime.Sub(stat.BeginTime).Seconds() * 1000.0
		//	r, _ := ctx.Value(recorderCtxKey{}).(*recorder)

	case *stats.OutPayload:
		r, _ := ctx.Value(recorderCtxKey{}).(*recorder)
		r.size += stat.WireLength
		fmt.Println("Using obw: ", r.name, r.size)
	}
}

func (env *BenchEnv) RegisterServers(server cppb.EgressIntraServiceServer, ig cppb.IngressIntraServiceServer, printBw bool) {
	sh := &statsHandler{}
	var tcpServer *grpc.Server
	if printBw {
		tcpServer = grpc.NewServer(grpc.StatsHandler(sh))
	} else {
		tcpServer = grpc.NewServer(grpc.MaxConcurrentStreams(5000000))
	}

	//is := &ingress.IngressServer{
	//	IncomingHandler: ingress.Handler{},
	//	RACManager:      env.RACManager,
	//}
	cppb.RegisterEgressIntraServiceServer(tcpServer, server)
	cppb.RegisterIngressIntraServiceServer(tcpServer, ig)
	var cleanup app.Cleanup
	g, _ := errgroup.WithContext(context.Background())
	g.Go(func() error {
		ip, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:33333")
		listener, _ := net.ListenTCP("tcp", ip)
		if err := tcpServer.Serve(listener); err != nil {
			return serrors.WrapStr("serving gRPC/TCP API", err)
		}
		return nil
	})
	cleanup.Add(func() error { tcpServer.GracefulStop(); return nil })
	env.tcpServer = tcpServer
}

func (env *BenchEnv) GetBeaconsSCION() []beacon.Beacon {
	return env.beacons
}
func (env *BenchEnv) BenchSCION() {
	start := time.Now()
	baseAlgo{}.SelectBeacons(env.GetBeaconsSCION(), 20)
	duration := time.Since(start)
	fmt.Println("[SCION] done in", duration.Nanoseconds())
}

//func (env *BenchEnv) GetBeaconsRAC() (*cppb.RACBeaconResponse, error) {
//	//storedBeacon := make([]*cppb.StoreadBeacon, 0, len(env.beacons))
//	//for i, b := range env.beacons {
//	//	// TODO: get rid of this overhead: raw-bytes in sql -> pathseg -> beacon -> pathseg proto -> raw bytes.
//	//	storedBeacon = append(storedBeacon, &cppb.IRECBeacon{
//	//		PathSeg: seg.PathSegmentToPB(b.Segment),
//	//		InIfId:  uint32(b.InIfId),
//	//		Id:      uint32(i),
//	//	})
//	//}
//	return &cppb.RACBeaconResponse{Beacons: env.irecbeacons}, nil
//}
//
//var wg sync.WaitGroup
//
//func (env *BenchEnv) BenchRac(iteration uint32) {
//	started := time.Now().UnixNano()
//	bcns, _ := env.GetBeaconsRAC()
//	//start := time.Now()
//	//fmt.Println("start at", time.Now().UnixNano())
//	env.RACManager.RunImm(context.Background(), &racservice.ExecutionRequest{
//		Beacons:       bcns.Beacons,
//		AlgorithmHash: []byte{0x00, 0xde, 0xad, 0xbe, 0xef},
//		Intfs:         nil,
//		PropIntfs:     []uint32{iteration, 1, 2},
//	}, &wg)
//	fmt.Println("\t[RAC]=s", iteration, ",", started)
//
//	wg.Wait()
//}
