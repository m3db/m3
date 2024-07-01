package rules

import (
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
)

// ServiceOptions configures the topic service.
type ServiceOptions interface {
	// ConfigService returns the client of config service.
	ConfigService() client.Client

	// SetConfigService sets the client of config service.
	SetConfigService(c client.Client) ServiceOptions

	// KVOverrideOptions returns the override options for KV store.
	KVOverrideOptions() kv.OverrideOptions

	// SetKVOverrideOptions sets the override options for KV store.
	SetKVOverrideOptions(value kv.OverrideOptions) ServiceOptions
}

type serviceOptions struct {
	configServiceClient client.Client
	kvOpts              kv.OverrideOptions
}

// NewServiceOptions returns new ServiceOptions.
func NewServiceOptions() ServiceOptions {
	return &serviceOptions{
		kvOpts: kv.NewOverrideOptions(),
	}
}

func (opts *serviceOptions) ConfigService() client.Client {
	return opts.configServiceClient
}

func (opts *serviceOptions) SetConfigService(c client.Client) ServiceOptions {
	o := *opts
	o.configServiceClient = c
	return &o
}

func (opts *serviceOptions) KVOverrideOptions() kv.OverrideOptions {
	return opts.kvOpts
}

func (opts *serviceOptions) SetKVOverrideOptions(value kv.OverrideOptions) ServiceOptions {
	o := *opts
	o.kvOpts = value
	return &o
}
