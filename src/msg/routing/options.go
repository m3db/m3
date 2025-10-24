package routing

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
)

// PolicyHandlerOptions provides options for creating a PolicyHandler.
type PolicyHandlerOptions interface {
	WithKVClient(kvClient client.Client) PolicyHandlerOptions
	KVClient() client.Client

	WithKVOverrideOptions(kvOverrideOptions kv.OverrideOptions) PolicyHandlerOptions
	KVOverrideOptions() kv.OverrideOptions

	WithPolicyConfig(policyConfig PolicyConfig) PolicyHandlerOptions
	PolicyConfig() PolicyConfig

	WithKVKey(kvKey string) PolicyHandlerOptions
	KVKey() string

	Validate() error
}

// NewPolicyHandlerOptions creates a new PolicyHandlerOptions.
func NewPolicyHandlerOptions() PolicyHandlerOptions {
	return &policyHandlerOptions{}
}

type policyHandlerOptions struct {
	kvClient          client.Client
	kvOverrideOptions kv.OverrideOptions
	kvKey             string
	policyConfig      PolicyConfig
}

func (o *policyHandlerOptions) WithKVClient(kvClient client.Client) PolicyHandlerOptions {
	o.kvClient = kvClient
	return o
}

func (o *policyHandlerOptions) KVClient() client.Client {
	return o.kvClient
}

func (o *policyHandlerOptions) WithKVOverrideOptions(kvOverrideOptions kv.OverrideOptions) PolicyHandlerOptions {
	o.kvOverrideOptions = kvOverrideOptions
	return o
}

func (o *policyHandlerOptions) KVOverrideOptions() kv.OverrideOptions {
	return o.kvOverrideOptions
}

func (o *policyHandlerOptions) WithPolicyConfig(policyConfig PolicyConfig) PolicyHandlerOptions {
	o.policyConfig = policyConfig
	return o
}

func (o *policyHandlerOptions) PolicyConfig() PolicyConfig {
	return o.policyConfig
}

func (o *policyHandlerOptions) WithKVKey(kvKey string) PolicyHandlerOptions {
	o.kvKey = kvKey
	return o
}

func (o *policyHandlerOptions) KVKey() string {
	return o.kvKey
}

func (o *policyHandlerOptions) Validate() error {
	if o.kvKey != "" {
		if o.kvClient == nil {
			return errors.New("kvClient is required if kvKey is set")
		}
		if err := o.kvOverrideOptions.Validate(); err != nil {
			return fmt.Errorf("kvOverride options is invalid: %w", err)
		}
	}
	if o.policyConfig == nil {
		return errors.New("policyConfig is required")
	}
	return nil
}
