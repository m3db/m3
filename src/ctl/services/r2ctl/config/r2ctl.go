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
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
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
func (c r2StoreConfiguration) NewR2Store(instrumentOpts instrument.Options) (r2.Store, error) {
	if c.Stub {
		return stub.NewStore(instrumentOpts), nil
	}

	if c.KV == nil {
		return nil, errKVConfigRequired
	}

	return c.KV.NewStore(instrumentOpts)
}

// kvStoreConfig is the configuration for the kv backed implementation of the R2 store.
type kvStoreConfig struct {
	// KVClient configures the client for key value store.
	KVClient *etcd.Configuration `yaml:"kvClient" validate:"nonzero"`

	// KVConfig configures the namespace and the environment.
	KVConfig clusterKV.Configuration `yaml:"kvConfig"`

	// Propagation delay for rule updates.
	PropagationDelay time.Duration `yaml:"propagationDelay" validate:"nonzero"`

	// NamespacesKey is where the namespace data lives in kv
	NamespacesKey string `yaml:"namespacesKey" validate:"nonzero"`

	// RuleSetKey fmt
	RuleSetKeyFmt string `yaml:"ruleSetKeyFmt" validate:"nonzero"`

	// Validation configuration.
	Validation *validationConfiguration `yaml:"validation"`
}

// NewStore creates a new kv backed store.
func (c kvStoreConfig) NewStore(instrumentOpts instrument.Options) (r2.Store, error) {
	// Create rules store.
	client, err := c.KVClient.NewClient(instrumentOpts)
	if err != nil {
		return nil, err
	}
	store, err := client.TxnStore(c.KVConfig.NewOptions())
	if err != nil {
		return nil, err
	}
	var validator rules.Validator
	if c.Validation != nil {
		validator = c.Validation.NewValidator()
	}
	rulesStoreOpts := rules.NewStoreOptions(c.NamespacesKey, c.RuleSetKeyFmt, validator)
	rulesStore := rules.NewStore(store, rulesStoreOpts)

	// Create kv store.
	kvStoreOpts := kv.NewStoreOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRuleUpdatePropagationDelay(c.PropagationDelay)
	return kv.NewStore(rulesStore, kvStoreOpts), nil
}

type validationConfiguration struct {
	MetricTypes metricTypesValidationConfiguration `yaml:"metricTypes"`
	Policies    policiesValidationConfiguration    `yaml:"policies"`
}

func (c validationConfiguration) NewValidator() rules.Validator {
	opts := rules.NewValidatorOptions().
		SetMetricTypesFn(c.MetricTypes.NewMetricTypesFn()).
		SetDefaultAllowedStoragePolicies(c.Policies.DefaultAllowed.StoragePolicies).
		SetDefaultAllowedCustomAggregationTypes(c.Policies.DefaultAllowed.AggregationTypes)
	for _, override := range c.Policies.Overrides {
		opts = opts.
			SetAllowedStoragePoliciesFor(override.Type, override.Allowed.StoragePolicies).
			SetAllowedCustomAggregationTypesFor(override.Type, override.Allowed.AggregationTypes)
	}
	return rules.NewValidator(opts)
}

type metricTypesValidationConfiguration struct {
	// Metric type tag.
	TypeTag string `yaml:"typeTag"`

	// Allowed metric types.
	Allowed []metric.Type `yaml:"allowed"`
}

func (c metricTypesValidationConfiguration) NewMetricTypesFn() rules.MetricTypesFn {
	return func(tagFilters map[string]string) ([]metric.Type, error) {
		allowed := make([]metric.Type, 0, len(c.Allowed))
		filterStr, exists := tagFilters[c.TypeTag]
		if !exists {
			// If there is not type filter provided, the filter may match any allowed type.
			allowed = append(allowed, c.Allowed...)
			return allowed, nil
		}
		f, err := filters.NewFilter([]byte(filterStr))
		if err != nil {
			return nil, err
		}
		for _, t := range c.Allowed {
			if f.Matches([]byte(t.String())) {
				allowed = append(allowed, t)
			}
		}
		return allowed, nil
	}
}

type policiesValidationConfiguration struct {
	// DefaultAllowed defines the policies allowed by default.
	DefaultAllowed policiesConfiguration `yaml:"defaultAllowed"`

	// Overrides define the metric type specific policy overrides.
	Overrides []policiesOverrideConfiguration `yaml:"overrides"`
}

type policiesOverrideConfiguration struct {
	Type    metric.Type           `yaml:"type"`
	Allowed policiesConfiguration `yaml:"allowed"`
}

type policiesConfiguration struct {
	StoragePolicies  []policy.StoragePolicy  `yaml:"storagePolicies"`
	AggregationTypes policy.AggregationTypes `yaml:"aggregationTypes"`
}
