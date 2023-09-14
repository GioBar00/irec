//
// Created by jelte on 9-6-23.
//
#ifndef UBPF_SHARED
#define UBPF_SHARED
#include "stdlib.h"
struct beacon_result {
    unsigned int beacon_id;
    unsigned int egress_intfs_len;
    unsigned int *egress_intfs;
};

struct execution_result {
    struct beacon_result* beacon;
    size_t result_len;
};
#define c_vec_len(V) (sizeof(V)/sizeof((V)[0]))
#endif
//
//
//
//struct PathSegmentUnsignedExtensions {
//};
//struct HopField{
//    uint64_t ingress;
//    uint64_t egress;
//    uint32_t exp_time;
//    size_t mac_len;
//    uint8_t *mac;
//};
//
//struct PeerEntry{
//    uint64_t peer_isd_as;
//    uint64_t peer_interface;
//    uint32_t peer_mtu;
//    struct HopField hop_field;
//};
//struct HopEntry{
//    struct HopField hop_field;
//    uint32_t ingress_mtu;
//};
//struct Digest{
//    size_t digest_len;
//    uint8_t *digest;
//};
//struct DigestExtension {
//    struct Digest epic;
//};
//struct HiddenPathExtension {
//    int is_hidden; // bool
//};
//struct BandwidthTuple {
//    uint64_t interface_identifier;
//    uint32_t latency;
//};
//
//
//struct BandwidthInfo {
//    size_t intra_len;
//    struct BandwidthTuple *intra;
//
//    size_t inter_len;
//    struct BandwidthTuple *inter;
//};
//
//struct LatencyTuple {
//    uint64_t interface_identifier;
//    uint32_t latency;
//};
//
//struct LatencyInfo {
//    size_t intra_len;
//    struct LatencyTuple *intra;
//
//    size_t inter_len;
//    struct LatencyTuple *inter;
//};
//
//struct StaticInfoExtension {
//    struct LatencyInfo latency;
//    struct BandwidthInfo bandwidth;
////  TODO: Maps for geo, link_type and internal_hops
//    char *note;
//    size_t note_len;
//};
//
//
//struct PathSegmentExtensions {
//    struct StaticInfoExtension static_info;
//    struct HiddenPathExtension hidden_path;
//    struct DigestExtension digests;
//};
//struct ASEntrySignedBody {
//    uint64_t isd_as;
//    uint64_t next_isd_as;
//    struct HopEntry hop_entry;
//    size_t peer_entries_len;
//    struct PeerEntry *peer_entries;
//    uint32_t mtu;
//    struct PathSegmentExtensions extensions;
//};
//
//struct EmiroASEntry {
//    struct ASEntrySignedBody signed_body;
//    struct PathSegmentUnsignedExtensions unsignedExts;
//};
//
//struct IRECPathSegment {
//    size_t segment_info_len;
//    size_t as_entries_len;
////    struct EmiroASEntry as_entries[];
//    uint8_t segment_info[];
//};
//struct IRECBeacon {
//    struct IRECPathSegment path_seg;
//    uint32_t in_info_id;
//    uint32_t id
//
//};

