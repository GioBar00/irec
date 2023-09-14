// Copyright 2021 Anapaya Systems
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

// Package beacon defines interfaces that extend the capabilities of a beacon storage compared to
// the beacon.DB interface. These additional capabilities are used outside of the beacon package.
// For example, they are used by the service management API.
package beacon

import (
	"github.com/scionproto/scion/pkg/addr"
)

type OriginOption struct {
	OriginAS        addr.IA  // must be set
	OriginIntfGroup []uint32 // optional, joined with OR
}
type AlgorithmOption struct {
	AlgHash []byte   // must be set
	AlgId   []uint32 // optional, joined with OR
}

type QueryOptions struct {
	Maximum      uint32 // Maximum amount of Beacons
	Algorithms   []AlgorithmOption
	Origins      []OriginOption // origin AS + origin interface group, joined with OR
	OnlyUnmarked bool           // used by static rac to ignore the markers placed by dynamic racs
	Labels       []uint32       // fetch beacons with specific labels.
	//TODO(jvb); groupBy ENUM (AlgHash, algId) ARR. Then a selectGroups = [1..]
}
