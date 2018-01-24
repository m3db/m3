package config

import "github.com/m3db/m3db/client"

// Configuration is the configuration for an instance of m3coordinator.
type Configuration struct {
	M3DBClientCfg client.Configuration `yaml:"client"`
}
