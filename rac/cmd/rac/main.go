package main

import (
	"github.com/scionproto/scion/private/app/launcher"
	"github.com/scionproto/scion/rac/config"
	_ "net/http/pprof"
)

var globalCfg config.Config

func main() {
	application := launcher.Application{
		TOMLConfig: &globalCfg,
		ShortName:  "SCION RAC",
		Main:       realMain,
	}
	application.Run()
}
