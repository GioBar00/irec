# Stdlib
from collections import defaultdict
import os

# External packages
import yaml
import argparse

from topology.defines import (
    GEN_PATH,
)
from topology.config import (
    DEFAULT_TOPOLOGY_FILE,
)
from topology.common import (
    LinkType,
)
from topology.topo import(
    LinkEP,
    IFIDGenerator
)

class EstResUsage(object):
    def __init__(self, topo_config_dict):
        self.topo_config_dict = topo_config_dict
        self.border_routers = defaultdict(list)
        self.br_by_as = {}
        self.control_services = {}
        self._read_links()
        self._calculate_cs()


    def _br_name(self, ep, assigned_br_id, br_ids, if_ids):
        br_name = ep.br_name()
        if br_name:
            # BR with multiple interfaces, reuse assigned id
            br_id = assigned_br_id.get(br_name)
            if br_id is None:
                # assign new id
                br_ids[ep] += 1
                assigned_br_id[br_name] = br_id = br_ids[ep]
        else:
            # BR with single interface
            br_ids[ep] += 1
            br_id = br_ids[ep]
        br = "br%s-%d" % (ep.file_fmt().replace("_", "-"), br_id)
        ifid = ep.ifid
        if not ifid:
            ifid = if_ids[ep].new()
        else:
            if_ids[ep].add(ifid)
        return br, ifid
    
    def _add_br(self, topo_id, br_name):
        topo_id = topo_id.file_fmt().replace("_", "-")
        if topo_id not in self.br_by_as:
            self.br_by_as[topo_id] = []
        if br_name not in self.border_routers:
            self.border_routers[br_name] = {}
            for link_type in LinkType:
                self.border_routers[br_name][link_type] = 0
            self.br_by_as[topo_id].append(br_name)
            

    def _read_links(self):
        assigned_br_id = {}
        br_ids = defaultdict(int)
        if_ids = defaultdict(lambda: IFIDGenerator())
        if not self.topo_config_dict.get("links", None):
            return
        for attrs in self.topo_config_dict["links"]:
            a = LinkEP(attrs.pop("a"))
            b = LinkEP(attrs.pop("b"))
            a_br, _ = self._br_name(a, assigned_br_id, br_ids, if_ids)
            b_br, _ = self._br_name(b, assigned_br_id, br_ids, if_ids)

            self._add_br(a, a_br)
            self._add_br(b, b_br)

            linkto = LinkType[attrs.pop("linkAtoB").upper()]
            if linkto == LinkType.CHILD:
                self.border_routers[a_br][LinkType.CHILD] += 1
                self.border_routers[b_br][LinkType.PARENT] += 1
            elif linkto == LinkType.PARENT:
                self.border_routers[a_br][LinkType.PARENT] += 1
                self.border_routers[b_br][LinkType.CHILD] += 1
            else:
                self.border_routers[a_br][linkto] += 1
                self.border_routers[b_br][linkto] += 1

    
    def _calculate_cs(self):
        for topo_id, brs in self.br_by_as.items():
            cs_name = "cs%s-1" % topo_id
            self.control_services[cs_name] = {}
            for link_type in LinkType:
                self.control_services[cs_name][link_type] = 0

            for br in brs:
                for link_type in LinkType:
                    self.control_services[cs_name][link_type] += self.border_routers[br][link_type]         


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--topo-config', default=DEFAULT_TOPOLOGY_FILE,
                        help='Path policy file')
    
    args = parser.parse_args()

    with open(args.topo_config) as f:
        topo_config = yaml.load(f, Loader=yaml.SafeLoader)

    est_res_usage = EstResUsage(topo_config)

    brs = {}
    for br in est_res_usage.border_routers.keys():
        brs[br] = {}
        brs[br]["cpu"] = 0
        brs[br]["memory"] = 0

    css = {}
    for cs in est_res_usage.control_services.keys():
        css[cs] = {}
        css[cs]["cpu"] = 0
        css[cs]["memory"] = 0

    sds = {}
    for topo_id in est_res_usage.br_by_as.keys():
        sd = "sd%s" % topo_id
        sds[sd] = {}
        sds[sd]["cpu"] = 0
        sds[sd]["memory"] = 0

    pods = {}
    other_pods = {}

    loops = 5

    for i in range(loops):
        print("Loop %d" % i)
        # run kubectl top pod --all-namespaces and get the cpu and memory usage for each pod
        # os.system("kubectl top pod --all-namespaces > logs/pod_usage.txt")

        with open("logs/pod_usage.txt") as f:
            lines = f.readlines()
            for line in lines[1:]:
                parts = line.split()
                namespace = parts[0]
                pod = parts[1]
                cpu = parts[2].replace("m", "")
                memory = parts[3].replace("Mi", "").replace("Gi", "000")
                if pod not in pods:
                    pods[pod] = {}
                    pods[pod]["cpu"] = 0
                    pods[pod]["memory"] = 0
                pods[pod]["cpu"] += int(cpu)
                pods[pod]["memory"] += int(memory)

        # wait for 1 seconds
        print("Sleeping for 1 seconds")
        os.system("sleep 1")

    for pod in pods.keys():
        pods[pod]["cpu"] = pods[pod]["cpu"] // loops
        pods[pod]["memory"] = pods[pod]["memory"] // loops
        # print("Pod: %s, CPU: %d, Memory: %d" % (pod, pods[pod]["cpu"], pods[pod]["memory"]))

    for pod in pods.keys():
        if pod.startswith("kathara-br"):
            br_name = "-".join(pod.split("-")[1:6])
            brs[br_name]["cpu"] = pods[pod]["cpu"]
            brs[br_name]["memory"] = pods[pod]["memory"]
        elif pod.startswith("kathara-cs"):
            cs_name = "-".join(pod.split("-")[1:6])
            css[cs_name]["cpu"] = pods[pod]["cpu"]
            css[cs_name]["memory"] = pods[pod]["memory"]
        elif pod.startswith("kathara-sd"):
            sd_name = "-".join(pod.split("-")[1:5])
            sds[sd_name]["cpu"] = pods[pod]["cpu"]
            sds[sd_name]["memory"] = pods[pod]["memory"]
        else:
            other_pods[pod] = pods[pod]

    print("Border Routers")
    for br in brs.keys():
        print("BR: %s, CPU: %d, Memory: %d" % (br, brs[br]["cpu"], brs[br]["memory"]))
    print("\n")

    print("Control Services")
    for cs in css.keys():
        print("CS: %s, CPU: %d, Memory: %d" % (cs, css[cs]["cpu"], css[cs]["memory"]))
    print("\n")

    print("Service Discoveries")
    for sd in sds.keys():
        print("SD: %s, CPU: %d, Memory: %d" % (sd, sds[sd]["cpu"], sds[sd]["memory"]))
    print("\n")

    print("Other Pods")
    for pod in other_pods.keys():
        print("Pod: %s, CPU: %d, Memory: %d" % (pod, other_pods[pod]["cpu"], other_pods[pod]["memory"]))
    print("\n")

    print("done")

if __name__ == "__main__":
    main()