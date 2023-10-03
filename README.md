# SCION

[![Slack chat](https://img.shields.io/badge/chat%20on-slack-blue?logo=slack)](https://scionproto.slack.com)
[![ReadTheDocs](https://img.shields.io/badge/doc-reference-blue?version=latest&style=flat&label=docs&logo=read-the-docs&logoColor=white)](https://docs.scion.org/en/latest)
[![Documentation](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/scionproto/scion)
[![Build Status](https://badge.buildkite.com/e7ca347d947c23883ad7c3a4d091c2df5ae7feb52b238d29a1.svg?branch=master)](https://buildkite.com/scionproto/scion)
[![Go Report Card](https://goreportcard.com/badge/github.com/scionproto/scion)](https://goreportcard.com/report/github.com/scionproto/scion)
[![GitHub issues](https://img.shields.io/github/issues/scionproto/scion/help%20wanted.svg?label=help%20wanted&color=purple)](https://github.com/scionproto/scion/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22)
[![GitHub issues](https://img.shields.io/github/issues/scionproto/scion/good%20first%20issue.svg?label=good%20first%20issue&color=purple)](https://github.com/scionproto/scion/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
[![Release](https://img.shields.io/github/release-pre/scionproto/scion.svg)](https://github.com/scionproto/scion/releases)
[![License](https://img.shields.io/github/license/scionproto/scion.svg?maxAge=2592000)](https://github.com/scionproto/scion/blob/master/LICENSE)



This is a work-in-progress repository for SCION using Inter-domain Routing with Extensible Criteria (IREC) for the beaconing infrastructure. Several variants of IREC execution environments have been identified and implemented using build tags. The following build tags are available.

## Build Tags
### ubpf
```make gazelle && make bazel```

Uses eBPF/uBPF as the RAC execution environment, where the eBPF code is interpreted while using memory checks.
### ubpfjit
```make gazelle-ubpfjit && make bazel-ubpfjit```

Uses eBPF/uBPF as the RAC execution environment. The code is Just-in-time compiled and executed. WARNING: Code is not verified as in the Linux eBPF virtual machine, do not run with untrusted code.
### native
```make gazelle-native && make bazel-native```

Uses a native execution environment, i.e. the RACs execute the Go-based selection algorithm directly. This can be used as an optimization for frequently used RACs. As the algorithm is fixed, this option ignores alhorithm hashes.

### wa
```make gazelle-wa && make bazel-wa```

WebAssembly optimized execution environment. The Ingress Gateway already includes the unpacked signed body in a job to the RAC. In the IRE-direction this is additional overhead, however in IRIE this can save data being transferred.

### waopt
```make gazelle-waopt && make bazel-waopt```

Confusing shorthand for WebAssembly with Optimizations stripped. The Ingress Gateway does not add the unpacked signed body, and this unpacking is done at the RAC.

### timing
For each of the above variants, there is a specific timing buildtag, which will include measurement code to measure time taken for certain functions. See also the ```bench``` package.

## IRE vs IRIE
There is a second (WIP) branch for IRIE direction of beacons. 

## Example Usage
Make sure to run the variants with the corresponding topology. The topology file specifies the RACs and which algorithms they should use. When using the incorrect topology file, the uBPF execution environment may attempt to run a WASM algorithm and fail. 

The algorithms that are registered at the ingress gateways can be adjusted accordingly in ```topology/default-rac.topo``` or ```topology/default-rac-wa.topo```

### Testing connectivity
A full end-to-end connectivity test can be done to ensure IREC is functioning correctly and beacons are propagated in the network.
```
./scion.sh stop
./scion.sh topology -c topology/default-rac.topo
make gazelle-ubpfjit
make bazel-ubpfjit
./scion.sh start
sleep 10
bin/end2end_integration
```

### Using pull-based beaconing:
```
bin\scion pullpaths 2-ff00:0:210 fb7a5d80b701842b5ab5f974b6500054b9473f824f884d63644257741b5109db --sciond $(./scion.sh sciond-addr 110)
```

This command will not return anything, but only initialize a pull based beacon.

The result of the pull-based beacon can be queried with
```
bin\scion showpaths 2-ff00:0:210 fb7a5d80b701842b5ab5f974b6500054b9473f824f884d63644257741b5109db --sciond $(./scion.sh sciond-addr 110)
```




# SCION
Welcome to the open-source implementation of
[SCION](http://www.scion-architecture.net) (Scalability, Control and Isolation
On next-generation Networks), a future Internet architecture. SCION is the first
clean-slate Internet architecture designed to provide route control, failure
isolation, and explicit trust information for end-to-end communication. To find
out more about the project, please visit our [documentation
site](https://docs.scion.org/en/latest/).

## Connecting to the SCION Test Network

Join [SCIONLab](https://www.scionlab.org) if you're interested in playing with
SCION in an operational global test deployment of SCION. As part of the SCIONLab
project, we support [pre-built binaries as Debian
packages](https://docs.scionlab.org/content/install/).

## Building

To find out how to work with SCION, please visit our [documentation
site](https://docs.scion.org/en/latest/contribute.html#setting-up-the-development-environment)
for instructions on how to install build dependencies, build and run SCION.

## Contributing

Interested in contribution to the SCION project? Please visit our
[contribution guide](https://docs.scion.org/en/latest/contribute.html)
for more information about how you can do so.

Join us on our [slack workspace](https://scionproto.slack.com) with this invite link:
[join scionproto.slack.com](https://join.slack.com/t/scionproto/shared_invite/zt-1gtgkuvk3-vQzq3gPOWOL6T58yu45vXg)

## License

[![License](https://img.shields.io/github/license/scionproto/scion.svg?maxAge=2592000)](https://github.com/scionproto/scion/blob/master/LICENSE)

./scion.sh stop && ./scion.sh topology -c topology/default-rac.topo && rm -rf logs/ && make gazelle-ubpfjit && make bazel-ubpfjit && ./scion.sh start && sleep 10 && bin/end2end_integration
