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

package validator

import (
	"errors"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/validator/namespace"
	"github.com/m3db/m3metrics/rules/validator/namespace/kv"
	"github.com/m3db/m3metrics/rules/validator/namespace/static"
)

var (
	errNoNamespaceValidatorConfiguration        = errors.New("no namespace validator configuration provided")
	errMultipleNamespaceValidatorConfigurations = errors.New("multiple namespace validator configurations provided")
)

// Configuration is the configuration for rules validation.
type Configuration struct {
	Namespace                        namespaceValidatorConfiguration    `yaml:"namespace"`
	RequiredRollupTags               []string                           `yaml:"requiredRollupTags"`
	MaxTransformationDerivativeOrder *int                               `yaml:"maxTransformationDerivativeOrder"`
	MaxRollupLevels                  *int                               `yaml:"maxRollupLevels"`
	MetricTypes                      metricTypesValidationConfiguration `yaml:"metricTypes"`
	Policies                         policiesValidationConfiguration    `yaml:"policies"`
	TagNameInvalidChars              string                             `yaml:"tagNameInvalidChars"`
	MetricNameInvalidChars           string                             `yaml:"metricNameInvalidChars"`
}

// NewValidator creates a new rules validator based on the given configuration.
func (c Configuration) NewValidator(
	kvClient client.Client,
) (rules.Validator, error) {
	nsValidator, err := c.Namespace.NewNamespaceValidator(kvClient)
	if err != nil {
		return nil, err
	}
	opts := NewOptions().
		SetNamespaceValidator(nsValidator).
		SetRequiredRollupTags(c.RequiredRollupTags).
		SetMetricTypesFn(c.MetricTypes.NewMetricTypesFn()).
		SetDefaultAllowedStoragePolicies(c.Policies.DefaultAllowed.StoragePolicies).
		SetDefaultAllowedFirstLevelAggregationTypes(c.Policies.DefaultAllowed.FirstLevelAggregationTypes).
		SetDefaultAllowedNonFirstLevelAggregationTypes(c.Policies.DefaultAllowed.NonFirstLevelAggregationTypes).
		SetTagNameInvalidChars(toRunes(c.TagNameInvalidChars)).
		SetMetricNameInvalidChars(toRunes(c.MetricNameInvalidChars))
	for _, override := range c.Policies.Overrides {
		opts = opts.
			SetAllowedStoragePoliciesFor(override.Type, override.Allowed.StoragePolicies).
			SetAllowedFirstLevelAggregationTypesFor(override.Type, override.Allowed.FirstLevelAggregationTypes).
			SetAllowedNonFirstLevelAggregationTypesFor(override.Type, override.Allowed.NonFirstLevelAggregationTypes)
	}
	if c.MaxTransformationDerivativeOrder != nil {
		opts = opts.SetMaxTransformationDerivativeOrder(*c.MaxTransformationDerivativeOrder)
	}
	if c.MaxRollupLevels != nil {
		opts = opts.SetMaxRollupLevels(*c.MaxRollupLevels)
	}
	return NewValidator(opts), nil
}

type namespaceValidatorConfiguration struct {
	KV     *kv.NamespaceValidatorConfiguration     `yaml:"kv"`
	Static *static.NamespaceValidatorConfiguration `yaml:"static"`
}

func (c namespaceValidatorConfiguration) NewNamespaceValidator(
	kvClient client.Client,
) (namespace.Validator, error) {
	if c.KV == nil && c.Static == nil {
		return nil, errNoNamespaceValidatorConfiguration
	}
	if c.KV != nil && c.Static != nil {
		return nil, errMultipleNamespaceValidatorConfigurations
	}
	if c.KV != nil {
		return c.KV.NewNamespaceValidator(kvClient)
	}
	return c.Static.NewNamespaceValidator(), nil
}

// metricTypesValidationConfiguration is th configuration for metric types validation.
type metricTypesValidationConfiguration struct {
	// Metric type tag.
	TypeTag string `yaml:"typeTag"`

	// Allowed metric types.
	Allowed []metric.Type `yaml:"allowed"`
}

// NewMetricTypesFn creates a new metric types fn from the given configuration.
func (c metricTypesValidationConfiguration) NewMetricTypesFn() MetricTypesFn {
	return func(tagFilters filters.TagFilterValueMap) ([]metric.Type, error) {
		allowed := make([]metric.Type, 0, len(c.Allowed))
		filterValue, exists := tagFilters[c.TypeTag]
		if !exists {
			// If there is not type filter provided, the filter may match any allowed type.
			allowed = append(allowed, c.Allowed...)
			return allowed, nil
		}
		f, err := filters.NewFilterFromFilterValue(filterValue)
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

// policiesValidationConfiguration is the configuration for policies validation.
type policiesValidationConfiguration struct {
	// DefaultAllowed defines the policies allowed by default.
	DefaultAllowed policiesConfiguration `yaml:"defaultAllowed"`

	// Overrides define the metric type specific policy overrides.
	Overrides []policiesOverrideConfiguration `yaml:"overrides"`
}

// policiesOverrideConfiguration is the configuration for metric type specific policy overrides.
type policiesOverrideConfiguration struct {
	Type    metric.Type           `yaml:"type"`
	Allowed policiesConfiguration `yaml:"allowed"`
}

// policiesConfiguration is the configuration for storage policies and aggregation types.
type policiesConfiguration struct {
	StoragePolicies               []policy.StoragePolicy `yaml:"storagePolicies"`
	FirstLevelAggregationTypes    []aggregation.Type     `yaml:"firstLevelAggregationTypes"`
	NonFirstLevelAggregationTypes []aggregation.Type     `yaml:"nonFirstLevelAggregationTypes"`
}

func toRunes(s string) []rune {
	r := make([]rune, 0, len(s))
	for _, c := range s {
		r = append(r, c)
	}
	return r
}
