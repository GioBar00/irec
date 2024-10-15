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

package beaconing

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	seg "github.com/scionproto/scion/pkg/segment"
	"github.com/scionproto/scion/private/topology"
)

// sortedIntfs returns all interfaces of the given link type sorted by interface
// ID.
func SortedIntfs(intfs *ifstate.Interfaces, linkType topology.LinkType) []uint16 {
	var result []uint16
	for ifid, intf := range intfs.All() {
		topoInfo := intf.TopoInfo()
		if topoInfo.LinkType != linkType {
			continue
		}
		result = append(result, ifid)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

type Summary struct {
	mu    sync.Mutex
	Srcs  map[addr.IA]struct{}
	ifIDs map[uint16]struct{}
	Count int
}

func newSummary() *Summary {
	return &Summary{
		Srcs:  make(map[addr.IA]struct{}),
		ifIDs: make(map[uint16]struct{}),
	}
}

func (s *Summary) AddSrc(ia addr.IA) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Srcs[ia] = struct{}{}
}

func (s *Summary) AddIfID(ifID uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ifIDs[ifID] = struct{}{}
}

func (s *Summary) Inc() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Count++
}

func (s *Summary) IfIDs() []uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]uint16, 0, len(s.ifIDs))
	for ifID := range s.ifIDs {
		list = append(list, ifID)
	}
	sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })
	return list
}

// hopDescription creates a human readable description of a AS entry list by
// describing the hops only.
func hopsDescription(entries []seg.ASEntry) string {
	var desc strings.Builder

	for _, entry := range entries {
		hop := entry.HopEntry.HopField
		if hop.ConsIngress != 0 {
			desc.WriteString(strconv.Itoa(int(hop.ConsIngress)))
			desc.WriteString(" ")
		}
		desc.WriteString(entry.Local.String())
		if hop.ConsEgress != 0 {
			desc.WriteString(" ")
			desc.WriteString(strconv.Itoa(int(hop.ConsIngress)))
			desc.WriteString(">")
		}

	}
	return desc.String()
}

// WithSilent creates a logger based on the logger in the context that only logs
// at debug level if silent is set. Otherwise, the logger in the context is
// returned.
func WithSilent(ctx context.Context, silent bool) log.Logger {
	if silent {
		return silentLogger{Logger: log.FromCtx(ctx)}
	}
	return log.FromCtx(ctx)
}

type silentLogger struct {
	log.Logger
}

func (s silentLogger) Info(msg string, ctx ...interface{}) {
	s.Logger.Debug(msg, ctx...)
}
func (s silentLogger) Error(msg string, ctx ...interface{}) {
	s.Logger.Debug(msg, ctx...)
}
