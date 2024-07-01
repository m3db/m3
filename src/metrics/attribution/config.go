package attribution

import (
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/metrics/attribution/rules"
)

// Config is the configuration for the attributor.
type Config struct {
	Enabled                 bool                     `yaml:"enabled"`
	CustomRulesKVKey        string                   `yaml:"customRulesKVKey"`
	KVOverrideConfiguration kv.OverrideConfiguration `yaml:"kvOverrideOptions"`
	KVWatchInitTimeout      *time.Duration           `yaml:"kvWatchInitTimeout"`
}

// NewAttributor creates a new Attributor using Config.
func (c *Config) NewAttributor(kvClient client.Client) (Attributor, error) {
	opts := NewOptions().
		SetCustomRulesKVKey(c.CustomRulesKVKey).
		SetKVWatchInitTimeout(*c.KVWatchInitTimeout)

	kvOverrideOptions, err := c.KVOverrideConfiguration.NewOverrideOptions()
	if err != nil {
		return nil, err
	}

	serviceOptions := rules.NewServiceOptions().
		SetKVOverrideOptions(kvOverrideOptions).
		SetConfigService(kvClient).
		SetKVOverrideOptions(kvOverrideOptions)

	ruleService, err := rules.NewService(serviceOptions)
	if err != nil {
		return nil, err
	}

	opts = opts.SetRulesService(ruleService)
	return NewAttributor(opts), nil
}
