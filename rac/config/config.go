package config

import (
	"github.com/scionproto/scion/private/env"
	"io"
	"net"
	"strconv"

	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/private/config"
)

const (
	defaultCtrlPort = 30256
)

type Config struct {
	Logging log.Config  `toml:"log,omitempty"`
	General env.General `toml:"general,omitempty"`
	RAC     RAC         `toml:"rac,omitempty"`
}

func (cfg *Config) InitDefaults() {
	config.InitAll(
		&cfg.General,
		&cfg.RAC,
		&cfg.Logging,
	)
}

func (cfg *Config) Validate() error {
	return config.ValidateAll(
		&cfg.General,
		&cfg.RAC,
		&cfg.Logging,
	)
}

func (cfg *Config) Sample(dst io.Writer, path config.Path, _ config.CtxMap) {
	config.WriteSample(dst, path, config.CtxMap{config.ID: "gateway"},
		&cfg.General,
		&cfg.RAC,
		&cfg.Logging,
	)
}

type RACAlgorithm struct {
	config.NoDefaulter
	config.NoValidator
	FilePath string `toml:"file,omitempty"`
}

func (cfg *RACAlgorithm) Sample(dst io.Writer, path config.Path, ctx config.CtxMap) {
	config.WriteString(dst, ``)
}

func (cfg *RACAlgorithm) ConfigName() string {
	return "local_algorithms"
}

type RAC struct {
	config.NoDefaulter
	Static          bool         `toml:"static,omitempty"`
	CtrlAddr        string       `toml:"ctrl_addr,omitempty"`
	RACAddr         string       `toml:"addr,omitempty"`
	StaticAlgorithm RACAlgorithm `toml:"static_algorithm,omitempty"`
}

func (cfg *RAC) Validate() error {
	cfg.CtrlAddr = DefaultAddress(cfg.CtrlAddr, defaultCtrlPort)
	return nil
}

func (cfg *RAC) Sample(dst io.Writer, path config.Path, ctx config.CtxMap) {
	config.WriteString(dst, ``)
}

func (cfg *RAC) ConfigName() string {
	return "rac"
}

func DefaultAddress(input string, defaultPort int) string {
	host, port, err := net.SplitHostPort(input)
	switch {
	case err != nil:
		return net.JoinHostPort(input, strconv.Itoa(defaultPort))
	case port == "0", port == "":
		return net.JoinHostPort(host, strconv.Itoa(defaultPort))
	default:
		return input
	}
}
