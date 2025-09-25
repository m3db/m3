package routing

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
)

type PolicyHandlerOptions interface {
	WithKVClient(kvClient client.Client) PolicyHandlerOptions
	KVClient() client.Client

	WithKVOverrideOptions(kvOverrideOptions kv.OverrideOptions) PolicyHandlerOptions
	KVOverrideOptions() kv.OverrideOptions

	WithStaticTrafficTypes(staticTrafficTypes map[string]uint64) PolicyHandlerOptions
	StaticTrafficTypes() map[string]uint64

	WithDynamicTrafficTypesKVKey(dynamicTrafficTypesKVKey string) PolicyHandlerOptions
	DynamicTrafficTypesKVKey() string

	Validate() error
}

func NewPolicyHandlerOptions() PolicyHandlerOptions {
	return &policyHandlerOptions{}
}

type policyHandlerOptions struct {
	kvClient                 client.Client
	kvOverrideOptions        kv.OverrideOptions
	dynamicTrafficTypesKVKey string
	staticTrafficTypes       map[string]uint64
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

func (o *policyHandlerOptions) WithStaticTrafficTypes(staticTrafficTypes map[string]uint64) PolicyHandlerOptions {
	o.staticTrafficTypes = staticTrafficTypes
	return o
}

func (o *policyHandlerOptions) StaticTrafficTypes() map[string]uint64 {
	return o.staticTrafficTypes
}

func (o *policyHandlerOptions) WithDynamicTrafficTypesKVKey(dynamicTrafficTypesKey string) PolicyHandlerOptions {
	o.dynamicTrafficTypesKVKey = dynamicTrafficTypesKey
	return o
}

func (o *policyHandlerOptions) DynamicTrafficTypesKVKey() string {
	return o.dynamicTrafficTypesKVKey
}

func (o *policyHandlerOptions) Validate() error {
	if o.kvClient == nil {
		return errors.New("kvClient is required")
	}
	if err := o.kvOverrideOptions.Validate(); err != nil {
		return fmt.Errorf("kvOverride options is invalid: %w", err)
	}
	if o.dynamicTrafficTypesKVKey == "" {
		return errors.New("dynamicTrafficTypesKVKey is required")
	}
	if o.staticTrafficTypes == nil {
		return errors.New("staticTrafficTypes is required")
	}
	return nil
}
