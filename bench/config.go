package main

import (
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/private/config"
	"github.com/scionproto/scion/private/env"
	"io"
)

type Config struct {
	Logging log.Config  `toml:"log,omitempty"`
	General env.General `toml:"general,omitempty"`
}

func (cfg *Config) InitDefaults() {
	config.InitAll(
		&cfg.General,
		&cfg.Logging,
	)
}

func (cfg *Config) Validate() error {
	return config.ValidateAll(
		&cfg.General,
		&cfg.Logging,
	)
}

func (cfg *Config) Sample(dst io.Writer, path config.Path, _ config.CtxMap) {
	config.WriteSample(dst, path, config.CtxMap{config.ID: "gateway"},
		&cfg.General,
		&cfg.Logging,
	)
}
