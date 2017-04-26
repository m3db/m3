package client

import "time"

// Configuration is the config for service discovery
type Configuration struct {
	InitTimeout time.Duration `yaml:"initTimeout"`
}

// NewOptions creates an Option
func (cfg Configuration) NewOptions() Options {
	opts := NewOptions()
	if cfg.InitTimeout != 0 {
		opts = opts.SetInitTimeout(cfg.InitTimeout)
	}
	return opts
}
