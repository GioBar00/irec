// Copyright 2018 Anapaya Systems
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

package integration

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/snet"
)

const (
	katharaCmd = "kathara"
)

var (
	// Kathara indicates if the tests should be executed in a Kathara
	Kathara = flag.Bool("k", false, "Run tests in Kathara")
)

var katharaArgs []string

func initKatharaArgs() {
	katharaArgs = []string{"exec", "-d", GenFile("kathara_lab")}
}

var _ Integration = (*katharaIntegration)(nil)

type katharaIntegration struct {
	*binaryIntegration
}

func katharize(bi *binaryIntegration) Integration {
	if *Kathara {
		return &katharaIntegration{
			binaryIntegration: bi,
		}
	}
	return bi
}

// StartServer starts a server and blocks until the ReadySignal is received on Stdout.
func (ki *katharaIntegration) StartServer(ctx context.Context, dst *snet.UDPAddr) (Waiter, error) {
	bi := *ki.binaryIntegration
	temp := append([]string{"env", fmt.Sprintf("%s=1", GoIntegrationEnv), "bash -c \"" + bi.cmd}, bi.serverArgs...)
	bi.serverArgs = append(katharaArgs, []string{EndhostID(dst), strings.Join(temp, " ") + "\""}...)
	bi.cmd = katharaCmd
	log.Debug(fmt.Sprintf("Starting server for %s in kathara",
		addr.FormatIA(dst.IA, addr.WithFileSeparator())),
	)
	return bi.StartServer(ctx, dst)
}

func (ki *katharaIntegration) StartClient(ctx context.Context,
	src, dst *snet.UDPAddr) (*BinaryWaiter, error) {
	bi := *ki.binaryIntegration
	temp := append([]string{"env", fmt.Sprintf("%s=1", GoIntegrationEnv), "bash -c \"" + bi.cmd}, bi.clientArgs...)
	bi.clientArgs = append(katharaArgs, []string{EndhostID(src), strings.Join(temp, " ") + "\""}...)
	bi.cmd = katharaCmd
	log.Debug(fmt.Sprintf("Starting client for %s in kathara",
		addr.FormatIA(src.IA, addr.WithFileSeparator())),
	)
	return bi.StartClient(ctx, src, dst)
}

// EndhostID returns the ID of the endhost container.
func EndhostID(a *snet.UDPAddr) string {
	ia := addr.FormatIA(a.IA, addr.WithFileSeparator())
	envID, ok := os.LookupEnv(fmt.Sprintf("sd%s", strings.Replace(ia, "-", "_", -1)))
	if !ok {
		return fmt.Sprintf("sd%s", strings.Replace(ia, "-", "_", -1))
	}
	return envID
}
