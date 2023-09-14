package egress

import (
	"github.com/scionproto/scion/control/ifstate"
	"github.com/scionproto/scion/private/topology"
	"sort"
)

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
