// Copyright 2020 Anapaya Systems
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

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/scionproto/scion/pkg/daemon"
	"time"

	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/pkg/private/serrors"
	"github.com/scionproto/scion/private/app"
	"github.com/scionproto/scion/private/app/flag"
	"github.com/scionproto/scion/private/tracing"
	"github.com/scionproto/scion/scion/showpaths"
	"github.com/spf13/cobra"
)

func newPullPaths(pather CommandPather) *cobra.Command {
	var envFlags flag.SCIONEnvironment
	var flags struct {
		timeout  time.Duration
		cfg      showpaths.Config
		extended bool
		json     bool
		logLevel string
		noColor  bool
		tracer   string
		format   string
	}

	var cmd = &cobra.Command{
		Use:     "pullpaths",
		Short:   "Pull paths to a SCION AS",
		Aliases: []string{"pp"},
		Args:    cobra.ExactArgs(2),
		Example: fmt.Sprintf(`  %[1]s pullpaths 1-ff00:0:110 fb7a5d80b701842b5ab5f974b6500054b9473f824f884d63644257741b5109db`, pather.CommandPath()),
		Long: fmt.Sprintf(`'pullpaths' aims to use pull-based beaconing to obtain a path between the local AS and a specific
target SCION AS. The path algorithm can be arbitrarily specified. Paths are subsequently used via the path server, i.e. for instance through a
showpaths command.

%s`, app.SequenceHelp),
		RunE: func(cmd *cobra.Command, args []string) error {
			dst, err := addr.ParseIA(args[0])
			if err != nil {
				return serrors.WrapStr("invalid destination ISD-AS", err)
			}

			algHash, err := hex.DecodeString(args[1])
			if err != nil {
				return serrors.WrapStr("invalid algorithm hash", err)
			}
			if err := app.SetupLog(flags.logLevel); err != nil {
				return serrors.WrapStr("setting up logging", err)
			}

			if err := envFlags.LoadExternalVars(); err != nil {
				return err
			}

			flags.cfg.Daemon = envFlags.Daemon()
			flags.cfg.Dispatcher = envFlags.Dispatcher()
			flags.cfg.Local = envFlags.Local().IPAddr().IP
			log.Debug("Resolved SCION environment flags",
				"daemon", flags.cfg.Daemon,
				"dispatcher", flags.cfg.Dispatcher,
				"local", flags.cfg.Local,
			)

			span, traceCtx := tracing.CtxWith(context.Background(), "run")
			span.SetTag("dst.isd_as", dst)
			defer span.Finish()

			ctx, cancel := context.WithTimeout(traceCtx, flags.timeout)
			defer cancel()

			sdConn, err := daemon.NewService(flags.cfg.Daemon).Connect(ctx)
			if err != nil {
				return serrors.WrapStr("connecting to the SCION Daemon", err, "addr", flags.cfg.Daemon)
			}
			defer sdConn.Close()
			localIA, err := sdConn.LocalIA(ctx)
			if err != nil {
				return serrors.WrapStr("determining local ISD-AS", err)
			}
			err = sdConn.PullPaths(ctx, dst, localIA,
				daemon.PullPathReqFlags{
					AlgorithmHash: algHash, //[]byte{0xFB, 0x7A, 0x5D, 0x80, 0xB7, 0x01, 0x84, 0x2B, 0x5A, 0xB5, 0xF9, 0x74, 0xB6, 0x50, 0x00, 0x54, 0xB9, 0x47, 0x3F, 0x82, 0x4F, 0x88, 0x4D, 0x63, 0x64, 0x42, 0x57, 0x74, 0x1B, 0x51, 0x09, 0xDB},
					AlgorithmId:   0,
				})

			if err != nil {
				return err
			}

			return nil
		},
	}

	envFlags.Register(cmd.Flags())
	cmd.Flags().DurationVar(&flags.timeout, "timeout", 5*time.Second, "Timeout")
	return cmd
}
