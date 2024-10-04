// Copyright 2019 Anapaya Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beacon

import (
	"context"
	"fmt"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"

	"github.com/scionproto/scion/pkg/addr"
	IREC "github.com/scionproto/scion/pkg/irec/includes/flatbuffers-go/irec"
	"github.com/scionproto/scion/pkg/private/common"
	"github.com/scionproto/scion/pkg/private/serrors"
	cppb "github.com/scionproto/scion/pkg/proto/control_plane"
	seg "github.com/scionproto/scion/pkg/segment"
)

const (
	// ErrReadingRows is the error message in case we fail to read more from
	// the database.
	ErrReadingRows common.ErrMsg = "Failed to read rows"
	// ErrParse is the error message in case the parsing a db entry fails.
	ErrParse common.ErrMsg = "Failed to parse entry"
)

type RacJobMetadata struct {
	RowID        int64
	PullBased    bool
	LastExecuted time.Time
	Count        int64
}

// InsertStats provides statistics about an insertion.
type InsertStats struct {
	Inserted, Updated, Filtered int
}

// DB defines the interface that all beacon DB backends have to implement.
type DB interface {
	// CandidateBeacons returns up to `setSize` beacons that are allowed for the
	// given usage. The beacons in the slice are ordered by segment length from
	// shortest to longest.
	CandidateBeacons(
		ctx context.Context,
		setSize int,
		usage Usage,
		src addr.IA,
	) ([]Beacon, error)
	// BeaconSources returns all source ISD-AS of the beacons in the database.
	BeaconSources(ctx context.Context) ([]addr.IA, error)
	// Insert inserts a beacon with its allowed usage into the database.
	InsertBeacon(ctx context.Context, beacon Beacon, usage Usage) (InsertStats, error)
}

const (
	// UsageUpReg indicates the beacon is allowed to be registered as an up segment.
	UsageUpReg Usage = 0x01
	// UsageDownReg indicates the beacon is allowed to be registered as a down segment.
	UsageDownReg Usage = 0x02
	// UsageCoreReg indicates the beacon is allowed to be registered as a core segment.
	UsageCoreReg Usage = 0x04
	// UsageProp indicates the beacon is allowed to be propagated.
	UsageProp Usage = 0x08
)

// Usage indicates what the beacon is allowed to be used for according to the policies.
type Usage int

// UsageFromPolicyType maps the policy type to the usage flag.
func UsageFromPolicyType(policyType PolicyType) Usage {
	switch policyType {
	case UpRegPolicy:
		return UsageUpReg
	case DownRegPolicy:
		return UsageDownReg
	case CoreRegPolicy:
		return UsageCoreReg
	case PropPolicy:
		return UsageProp
	default:
		panic(fmt.Sprintf("Invalid policyType: %v", policyType))
	}
}

// None indicates whether the beacons is not allowed to be used anywhere.
func (u Usage) None() bool {
	return u&0x0F == 0
}

func (u Usage) String() string {
	names := []string{}
	if u&UsageUpReg != 0 {
		names = append(names, "UpRegistration")
	}
	if u&UsageDownReg != 0 {
		names = append(names, "DownRegistration")
	}
	if u&UsageCoreReg != 0 {
		names = append(names, "CoreRegistration")
	}
	if u&UsageProp != 0 {
		names = append(names, "Propagation")
	}
	return fmt.Sprintf("Usage: [%s]", strings.Join(names, ","))
}

func PackBeaconFB(beacon *Beacon) ([]byte, error) {
	ps := beacon.Segment
	builder := flatbuffers.NewBuilder(0)
	ase_offsets := make([]flatbuffers.UOffsetT, len(ps.ASEntries))
	for in, ase := range ps.ASEntries {
		var si_ext flatbuffers.UOffsetT
		//Static info geo
		if ase.Extensions.StaticInfo != nil {
			var si_geo flatbuffers.UOffsetT
			var si_bw_intra flatbuffers.UOffsetT
			var si_bw_inter flatbuffers.UOffsetT
			var si_la_intra flatbuffers.UOffsetT
			var si_la_inter flatbuffers.UOffsetT
			var si_ih flatbuffers.UOffsetT
			var si_lt flatbuffers.UOffsetT
			if ase.Extensions.StaticInfo.Geo != nil {
				geo_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.Geo))
				index := 0
				for k, geo := range ase.Extensions.StaticInfo.Geo {
					address := builder.CreateString(geo.Address)
					IREC.GeoCoordinatesStart(builder)
					IREC.GeoCoordinatesAddAddress(builder, address)
					IREC.GeoCoordinatesAddIfId(builder, uint64(k))
					IREC.GeoCoordinatesAddLatitude(builder, geo.Latitude)
					IREC.GeoCoordinatesAddLongitude(builder, geo.Longitude)
					geo_list[index] = IREC.GeoCoordinatesEnd(builder)
					index++
				}
				IREC.StaticInfoExtStartGeoVector(builder, index)
				for _, geo := range geo_list {
					builder.PrependUOffsetT(geo)
				}
				si_geo = builder.EndVector(index)
			}
			if ase.Extensions.StaticInfo.Bandwidth.Intra != nil {
				bw_intra_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.Bandwidth.Intra))
				index := 0
				for k, intra := range ase.Extensions.StaticInfo.Bandwidth.Intra {
					bw_intra_list[index] = IREC.CreateMapEntryUlongUlong(builder, uint64(k), intra)
					index++
				}

				IREC.BandwidthInfoStartIntraVector(builder, len(ase.Extensions.StaticInfo.Bandwidth.Intra))
				for _, intra := range bw_intra_list {
					//irec.BandwidthInfoAddIntra(builder, intra)
					builder.PrependUOffsetT(intra)
				}
				si_bw_intra = builder.EndVector(len(bw_intra_list))
			}
			if ase.Extensions.StaticInfo.Bandwidth.Inter != nil {
				bw_inter_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.Bandwidth.Inter))
				index := 0
				for k, inter := range ase.Extensions.StaticInfo.Bandwidth.Inter {
					bw_inter_list[index] = IREC.CreateMapEntryUlongUlong(builder, uint64(k), inter)
					index++
				}

				IREC.BandwidthInfoStartIntraVector(builder, len(ase.Extensions.StaticInfo.Bandwidth.Intra))
				for _, inter := range bw_inter_list {
					//irec.BandwidthInfoAddInter(builder, inter)
					builder.PrependUOffsetT(inter)
				}
				si_bw_inter = builder.EndVector(len(bw_inter_list))

			}
			IREC.BandwidthInfoStart(builder)
			if ase.Extensions.StaticInfo.Bandwidth.Inter != nil {
				IREC.BandwidthInfoAddInter(builder, si_bw_inter)
			}
			if ase.Extensions.StaticInfo.Bandwidth.Intra != nil {
				IREC.BandwidthInfoAddIntra(builder, si_bw_intra)
			}
			bw := IREC.BandwidthInfoEnd(builder)

			if ase.Extensions.StaticInfo.Latency.Intra != nil {
				la_intra_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.Latency.Intra))
				index := 0
				for k, intra := range ase.Extensions.StaticInfo.Latency.Intra {
					la_intra_list[index] = IREC.CreateMapEntryUlongUlong(builder, uint64(k), uint64(intra))
					index++
				}

				IREC.LatencyInfoStartIntraVector(builder, len(ase.Extensions.StaticInfo.Latency.Intra))
				for _, intra := range la_intra_list {
					//irec.BandwidthInfoAddIntra(builder, intra)
					builder.PrependUOffsetT(intra)
				}
				si_la_intra = builder.EndVector(len(la_intra_list))
			}
			if ase.Extensions.StaticInfo.Latency.Inter != nil {
				la_inter_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.Latency.Inter))
				index := 0
				for k, inter := range ase.Extensions.StaticInfo.Latency.Inter {
					la_inter_list[index] = IREC.CreateMapEntryUlongUlong(builder, uint64(k), uint64(inter))
					index++
				}

				IREC.LatencyInfoStartInterVector(builder, len(ase.Extensions.StaticInfo.Latency.Intra))
				for _, inter := range la_inter_list {
					//irec.BandwidthInfoAddInter(builder, inter)
					builder.PrependUOffsetT(inter)
				}
				si_la_inter = builder.EndVector(len(la_inter_list))

			}
			IREC.LatencyInfoStart(builder)
			if ase.Extensions.StaticInfo.Latency.Inter != nil {
				IREC.LatencyInfoAddInter(builder, si_la_inter)
			}
			if ase.Extensions.StaticInfo.Latency.Intra != nil {
				IREC.LatencyInfoAddIntra(builder, si_la_intra)
			}
			lat := IREC.LatencyInfoEnd(builder)

			if ase.Extensions.StaticInfo.InternalHops != nil {

				ih_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.InternalHops))
				index := 0
				for k, ih := range ase.Extensions.StaticInfo.InternalHops {
					ih_list[index] = IREC.CreateMapEntryUlongUint(builder, uint64(k), ih)
					index++
				}

				IREC.StaticInfoExtStartInternalHopsVector(builder, len(ase.Extensions.StaticInfo.InternalHops))
				for _, ih := range ih_list {
					builder.PrependUOffsetT(ih)
				}
				si_ih = builder.EndVector(len(ih_list))

			}

			if ase.Extensions.StaticInfo.LinkType != nil {

				lt_list := make([]flatbuffers.UOffsetT, len(ase.Extensions.StaticInfo.LinkType))
				index := 0
				for k, lt := range ase.Extensions.StaticInfo.LinkType {
					lt_list[index] = IREC.CreateMapEntryUlongUint(builder, uint64(k), uint32(lt))
					index++
				}

				IREC.StaticInfoExtStartLinkTypeVector(builder, len(ase.Extensions.StaticInfo.LinkType))
				for _, ih := range lt_list {
					builder.PrependUOffsetT(ih)
				}
				si_lt = builder.EndVector(len(lt_list))

			}

			note := builder.CreateString(ase.Extensions.StaticInfo.Note)
			IREC.StaticInfoExtStart(builder)
			IREC.StaticInfoExtAddGeo(builder, si_geo)
			IREC.StaticInfoExtAddBandwidth(builder, bw)
			IREC.StaticInfoExtAddNote(builder, note)
			IREC.StaticInfoExtAddLatency(builder, lat)
			IREC.StaticInfoExtAddInternalHops(builder, si_ih)
			IREC.StaticInfoExtAddInternalHops(builder, si_lt)
			si_ext = IREC.StaticInfoExtEnd(builder)
		}

		IREC.SignedExtensionsStart(builder)
		hiddenpath_ext := IREC.CreateHiddenPathExt(builder, ase.Extensions.HiddenPath.IsHidden)
		IREC.SignedExtensionsAddHiddenPath(builder, hiddenpath_ext)
		if ase.Extensions.StaticInfo != nil {
			IREC.SignedExtensionsAddStaticInfo(builder, si_ext)
		}
		if ase.Extensions.Irec != nil {
			irec_ext := IREC.CreateIrecExt(builder, uint32(ase.Extensions.Irec.InterfaceGroup))
			IREC.SignedExtensionsAddIrec(builder, irec_ext)
		}
		signed_exts := IREC.SignedExtensionsEnd(builder)

		peer_entries_list := make([]flatbuffers.UOffsetT, len(ase.PeerEntries))
		for k, pe := range ase.PeerEntries {
			peer_entries_list[k] = IREC.CreatePeerEntry(builder, uint64(pe.Peer), uint64(pe.PeerInterface), uint32(pe.PeerMTU), uint64(pe.HopField.ConsIngress), uint64(pe.HopField.ConsEgress), uint32(pe.HopField.ExpTime))
		}
		IREC.ASEntryStartPeerEntriesVector(builder, len(peer_entries_list))
		for _, pe := range peer_entries_list {
			builder.PrependUOffsetT(pe)
		}
		peer_entries := builder.EndVector(len(peer_entries_list))

		IREC.ASEntryStart(builder)
		IREC.ASEntryAddIsdAs(builder, uint64(ase.Local))
		IREC.ASEntryAddNextIsdAs(builder, uint64(ase.Next))
		IREC.ASEntryAddExtensions(builder, signed_exts)
		IREC.ASEntryAddMtu(builder, uint32(ase.MTU))
		IREC.ASEntryAddHeIngressMtu(builder, uint32(ase.HopEntry.IngressMTU))
		hf := IREC.CreateHopField(builder, uint64(ase.HopEntry.HopField.ConsIngress), uint64(ase.HopEntry.HopField.ConsEgress), uint32(ase.HopEntry.HopField.ExpTime))

		IREC.ASEntryAddHopField(builder, hf)
		IREC.ASEntryAddPeerEntries(builder, peer_entries)
		// TODO(jvb): irec.ASEntryAddUnsignedExtensions(builder, unsigned_exts)

		ase_offsets[in] = IREC.ASEntryEnd(builder)
	}

	IREC.BeaconStartAsEntriesVector(builder, len(ase_offsets))
	for _, offset := range ase_offsets {
		builder.PrependUOffsetT(offset)
	}
	beacon_ase := builder.EndVector(len(ase_offsets))
	IREC.BeaconStart(builder)
	IREC.BeaconAddAsEntries(builder, beacon_ase)
	//irec.BeaconAddId(builder, )                         // TODO; id
	IREC.BeaconAddInIfId(builder, uint32(beacon.InIfID))
	packed := IREC.BeaconEnd(builder)
	builder.Finish(packed)
	bytes := builder.FinishedBytes()
	return bytes, nil
}

// todo(jvb); get rid of this, this was only an optimization for wasm. waopt
func PackBeaconIREC(ps *seg.PathSegment) ([]byte, error) {
	if ps == nil {
		return []byte{}, serrors.New("ps cannot be nill")
	}

	pb := &cppb.IRECPathSegment{
		SegmentInfo: ps.Info.Raw,
		AsEntries:   make([]*cppb.IRECASEntry, 0, len(ps.ASEntries)),
	}
	for _, entry := range ps.ASEntries {
		pb.AsEntries = append(pb.AsEntries, &cppb.IRECASEntry{
			Signed:     entry.Signed,
			SignedBody: entry.SignedBody,
			Unsigned:   seg.UnsignedExtensionsToPB(entry.UnsignedExtensions),
		})
	}
	return proto.Marshal(pb)
}

func UnpackBeaconIREC(raw []byte) (*cppb.IRECPathSegment, error) {
	var pb cppb.IRECPathSegment
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, err
	}
	return &pb, nil
}

func UnpackBeaconIRECExcerpt(raw []byte) (*cppb.IRECPathSegmentExcerpt, error) {
	var pb cppb.IRECPathSegmentExcerpt
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, err
	}
	return &pb, nil
}

// PackBeacon packs a beacon.
func PackBeacon(ps *seg.PathSegment) ([]byte, error) {

	return proto.Marshal(seg.PathSegmentToPB(ps))

}

// UnpackBeacon unpacks a beacon.
func UnpackBeaconPB(raw []byte) (*cppb.PathSegment, error) {
	var pb cppb.PathSegment
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, err
	}
	return &pb, nil
}

// UnpackBeacon unpacks a beacon.
func UnpackBeacon(raw []byte) (*seg.PathSegment, error) {
	var pb cppb.PathSegment
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return nil, err
	}
	return seg.BeaconFromPB(&pb)
}
