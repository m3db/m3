// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/cluster/client/etcd"
	clusterkv "github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/ctl/auth"
	r2store "github.com/m3db/m3/src/ctl/service/r2/store"
	r2kv "github.com/m3db/m3/src/ctl/service/r2/store/kv"
	"github.com/m3db/m3/src/ctl/service/r2/store/stub"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/metrics/rules/validator"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/log"
)

var (
	errKVConfigRequired = errors.New("must provide kv configuration if not using stub store")
)

// Configuration is the global configuration for r2ctl.
type Configuration struct {
	// Logging configuration.
	Logging log.Configuration `yaml:"logging"`

	// HTTP server configuration.
	HTTP serverConfig `yaml:"http"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Store configuration.
	Store r2StoreConfiguration `yaml:"store"`

	// Simple Auth Config.
	Auth *auth.SimpleAuthConfig `yaml:"auth"`
}

// r2StoreConfiguration has all the fields necessary for an R2 store.
type r2StoreConfiguration struct {
	// Stub means use the stub store.
	Stub bool `yaml:"stub"`

	// KV is the configuration for the etcd backed implementation of the kv store.
	KV *kvStoreConfig `yaml:"kv,omitempty"`
}

// NewR2Store creates a new R2 store.
func (c r2StoreConfiguration) NewR2Store(instrumentOpts instrument.Options) (r2store.Store, error) {
	if c.Stub {
		return stub.NewStore(instrumentOpts)
	}

	if c.KV == nil {
		return nil, errKVConfigRequired
	}

	return c.KV.NewStore(instrumentOpts)
}

// kvStoreConfig is the configuration for the KV backed implementation of the R2 store.
type kvStoreConfig struct {
	// KVClient configures the client for key value store.
	KVClient *etcd.Configuration `yaml:"kvClient" validate:"nonzero"`

	// KV configuration for the rules store.
	KVConfig clusterkv.OverrideConfiguration `yaml:"kvConfig"`

	// NamespacesKey is KV key associated with namespaces..
	NamespacesKey string `yaml:"namespacesKey" validate:"nonzero"`

	// RuleSet key format.
	RuleSetKeyFmt string `yaml:"ruleSetKeyFmt" validate:"nonzero"`

	// Propagation delay for rule updates.
	PropagationDelay time.Duration `yaml:"propagationDelay" validate:"nonzero"`

	// Validation configuration.
	Validation *validator.Configuration `yaml:"validation"`
}

// NewStore creates a new KV backed R2 store.
func (c kvStoreConfig) NewStore(instrumentOpts instrument.Options) (r2store.Store, error) {
	// Create rules store.
	kvClient, err := c.KVClient.NewClient(instrumentOpts)
	if err != nil {
		return nil, err
	}
	kvOpts, err := c.KVConfig.NewOverrideOptions()
	if err != nil {
		return nil, err
	}
	kvStore, err := kvClient.TxnStore(kvOpts)
	if err != nil {
		return nil, err
	}
	var validator rules.Validator
	if c.Validation != nil {
		validator, err = c.Validation.NewValidator(kvClient)
		if err != nil {
			return nil, err
		}
	}
	rulesStoreOpts := ruleskv.NewStoreOptions(c.NamespacesKey, c.RuleSetKeyFmt, validator)
	rulesStore := ruleskv.NewStore(kvStore, rulesStoreOpts)

	// Create kv store.
	r2StoreOpts := r2kv.NewStoreOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRuleUpdatePropagationDelay(c.PropagationDelay).
		SetValidator(validator)
	return r2kv.NewStore(rulesStore, r2StoreOpts), nil
}
