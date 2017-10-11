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

	"github.com/m3db/m3cluster/client/etcd"
	clusterKV "github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3ctl/r2"
	"github.com/m3db/m3ctl/r2/kv"
	"github.com/m3db/m3ctl/r2/stub"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
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
	Store R2StoreConfiguration `yaml:"store"`

	// Simple Auth Config.
	Auth *auth.SimpleAuthConfig `yaml:"auth"`

	// Clock configuration used by the api.
	Clock clock.Configuration `yaml:"clock"`
}

// R2StoreConfiguration has all the fields necessary for an R2Store
type R2StoreConfiguration struct {
	// Stub means use the stub store.
	Stub bool `yaml:"stub"`

	// KV is the configuration for the etcd backed implementation of the kvStore.
	KV *KVStoreConfig `yaml:"kv,omitempty"`
}

// KVStoreConfig is the configuration for the etcd backed implementation of the kvStore.
type KVStoreConfig struct {
	// KV namespace.
	RulesKVConfig clusterKV.Configuration `yaml:"rulesKVConfig"`

	// KVClient configuration for key value store.
	KVClient *etcd.Configuration `yaml:"etcd" validate:"nonzero"`

	// Propagation delay for rule updates.
	PropagationDelay time.Duration `yaml:"propagationDelay" validate:"nonzero"`

	// NamespacesKey is where the namespace data lives in kv
	NamespacesKey string `yaml:"namespacesKey" validate:"nonzero"`

	// RuleSetKey fmt
	RuleSetKeyFmt string `yaml:"ruleSetKeyFmt" validate:"nonzero"`
}

// NewR2Store creates
func (c R2StoreConfiguration) NewR2Store(iOpts instrument.Options, clockOpts clock.Options) (r2.Store, error) {
	if c.Stub {
		return stub.NewStore(iOpts), nil
	}

	if c.KV == nil {
		return nil, errKVConfigRequired
	}

	client, err := c.KV.KVClient.NewClient(iOpts)
	if err != nil {
		return nil, err
	}

	store, err := client.TxnStore(c.KV.RulesKVConfig.NewOptions())
	if err != nil {
		return nil, err
	}

	r2StoreOpts := kv.NewStoreOptions().
		SetInstrumentOptions(iOpts).
		SetClockOptions(clockOpts)

	storeOpts := rules.NewStoreOptions(c.KV.NamespacesKey, c.KV.RuleSetKeyFmt)
	kvStore := rules.NewStore(store, storeOpts)
	updateHelper := rules.NewRuleSetUpdateHelper(c.KV.PropagationDelay)

	return kv.NewStore(kvStore, updateHelper, r2StoreOpts), nil
}
