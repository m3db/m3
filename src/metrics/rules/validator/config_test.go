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
	"testing"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestNamespaceValidatorConfigurationNoConfigurationProvided(t *testing.T) {
	cfgStr := ""
	var cfg namespaceValidatorConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))
	_, err := cfg.NewNamespaceValidator(nil)
	require.Equal(t, errNoNamespaceValidatorConfiguration, err)
}

func TestNamespaceValidatorConfigurationMultipleConfigurationProvided(t *testing.T) {
	cfgStr := `
kv:
  kvConfig:
    zone: testZone
    environment: testEnvironment
  initWatchTimeout: 5ms
  validNamespacesKey: testValidNamespaces
static:
  validationResult: valid
`
	var cfg namespaceValidatorConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))
	_, err := cfg.NewNamespaceValidator(nil)
	require.Equal(t, errMultipleNamespaceValidatorConfigurations, err)
}

func TestNamespaceValidatorConfigurationKV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfgStr := `
kv:
  kvConfig:
    zone: testZone
    environment: testEnvironment
  initWatchTimeout: 5ms
  validNamespacesKey: testValidNamespaces
`
	var cfg namespaceValidatorConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))

	kvStore := mem.NewStore()
	kvOpts := kv.NewOverrideOptions().SetZone("testZone").SetEnvironment("testEnvironment")
	kvClient := client.NewMockClient(ctrl)
	kvClient.EXPECT().Store(kvOpts).Return(kvStore, nil)
	_, err := cfg.NewNamespaceValidator(kvClient)
	require.NoError(t, err)
}

func TestNewValidator(t *testing.T) {
	cfgStr := `
namespace:
  static:
    validationResult: valid
requiredRollupTags:
  - tag1
  - tag2
maxTransformationDerivativeOrder: 2
maxRollupLevels: 1
metricTypes:
  typeTag: type
  allowed:
    - counter
    - timer
    - gauge
policies:
  defaultAllowed:
    storagePolicies:
      - 10s:2d
      - 1m:40d
    nonFirstLevelAggregationTypes:
      - Sum
      - Last
  overrides:
    - type: counter
      allowed:
        firstLevelAggregationTypes:
          - Sum
    - type: timer
      allowed:
        storagePolicies:
          - 10s:2d
        firstLevelAggregationTypes:
          - P50
          - P9999
    - type: gauge
      allowed:
        firstLevelAggregationTypes:
          - Last
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))
	opts := cfg.newValidatorOptions(nil)

	inputs := []struct {
		metricType                      metric.Type
		allowedStoragePolicies          policy.StoragePolicies
		disallowedStoragePolicies       policy.StoragePolicies
		allowedFirstLevelAggTypes       aggregation.Types
		disallowedFirstLevelAggTypes    aggregation.Types
		allowedNonFirstLevelAggTypes    aggregation.Types
		disallowedNonFirstLevelAggTypes aggregation.Types
	}{
		{
			metricType: metric.CounterType,
			allowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("10s:2d"),
				policy.MustParseStoragePolicy("1m:40d"),
			},
			disallowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("1m:2d"),
				policy.MustParseStoragePolicy("10s:40d"),
			},
			allowedFirstLevelAggTypes: aggregation.Types{
				aggregation.Sum,
			},
			disallowedFirstLevelAggTypes: aggregation.Types{
				aggregation.Last,
			},
			allowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Sum,
				aggregation.Last,
			},
			disallowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Min,
				aggregation.P99,
			},
		},
		{
			metricType: metric.TimerType,
			allowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("10s:2d"),
			},
			disallowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("1m:2d"),
				policy.MustParseStoragePolicy("1m:40d"),
			},
			allowedFirstLevelAggTypes: aggregation.Types{
				aggregation.P50,
				aggregation.P9999,
			},
			disallowedFirstLevelAggTypes: aggregation.Types{
				aggregation.Last,
			},
			allowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Sum,
				aggregation.Last,
			},
			disallowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Min,
				aggregation.P99,
			},
		},
		{
			metricType: metric.GaugeType,
			allowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("10s:2d"),
				policy.MustParseStoragePolicy("1m:40d"),
			},
			disallowedStoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("1m:2d"),
				policy.MustParseStoragePolicy("10s:40d"),
			},
			allowedFirstLevelAggTypes: aggregation.Types{
				aggregation.Last,
			},
			disallowedFirstLevelAggTypes: aggregation.Types{
				aggregation.Sum,
			},
			allowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Sum,
				aggregation.Last,
			},
			disallowedNonFirstLevelAggTypes: aggregation.Types{
				aggregation.Min,
				aggregation.P99,
			},
		},
	}

	for _, input := range inputs {
		for _, storagePolicy := range input.allowedStoragePolicies {
			require.True(t, opts.IsAllowedStoragePolicyFor(input.metricType, storagePolicy))
		}
		for _, storagePolicy := range input.disallowedStoragePolicies {
			require.False(t, opts.IsAllowedStoragePolicyFor(input.metricType, storagePolicy))
		}
		for _, aggregationType := range input.allowedFirstLevelAggTypes {
			require.True(t, opts.IsAllowedFirstLevelAggregationTypeFor(input.metricType, aggregationType))
		}
		for _, aggregationType := range input.disallowedFirstLevelAggTypes {
			require.False(t, opts.IsAllowedFirstLevelAggregationTypeFor(input.metricType, aggregationType))
		}
		for _, aggregationType := range input.allowedNonFirstLevelAggTypes {
			require.True(t, opts.IsAllowedNonFirstLevelAggregationTypeFor(input.metricType, aggregationType))
		}
		for _, aggregationType := range input.disallowedNonFirstLevelAggTypes {
			require.False(t, opts.IsAllowedNonFirstLevelAggregationTypeFor(input.metricType, aggregationType))
		}
	}
}

func TestNamespaceValidatorConfigurationStatic(t *testing.T) {
	cfgStr := `
static:
  validationResult: valid
`
	var cfg namespaceValidatorConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))
	_, err := cfg.NewNamespaceValidator(nil)
	require.NoError(t, err)
}

func TestConfigurationRequiredRollupTags(t *testing.T) {
	cfg := `
requiredRollupTags:
  - tag1
  - tag2
`
	var c Configuration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []string{"tag1", "tag2"}, c.RequiredRollupTags)
}

func TestConfigurationTagNameInvalidChars(t *testing.T) {
	cfg := `tagNameInvalidChars: "%\n"`
	var c Configuration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []rune{'%', '\n'}, toRunes(c.TagNameInvalidChars))
}

func TestConfigurationMetricNameInvalidChars(t *testing.T) {
	cfg := `metricNameInvalidChars: "%\n"`
	var c Configuration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []rune{'%', '\n'}, toRunes(c.MetricNameInvalidChars))
}

func TestNewMetricTypesFn(t *testing.T) {
	cfg := `
typeTag: type
allowed:
  - counter
  - timer
  - gauge
`

	var c metricTypesValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	fn := c.NewMetricTypesFn()

	inputs := []struct {
		filters       filters.TagFilterValueMap
		expectedTypes []metric.Type
	}{
		{
			filters:       nil,
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType, metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"randomTag": filters.FilterValue{Pattern: "counter"},
			},
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType, metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "counter"},
			},
			expectedTypes: []metric.Type{metric.CounterType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "timer"},
			},
			expectedTypes: []metric.Type{metric.TimerType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "gauge"},
			},
			expectedTypes: []metric.Type{metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "*er"},
			},
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType},
		},
	}

	for _, input := range inputs {
		res, err := fn(input.filters)
		require.NoError(t, err)
		require.Equal(t, input.expectedTypes, res)
	}
}

func TestNewMetricTypesFnError(t *testing.T) {
	cfg := `
typeTag: type
allowed:
  - counter
  - timer
  - gauge
`

	var c metricTypesValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	fn := c.NewMetricTypesFn()

	inputs := []filters.TagFilterValueMap{
		filters.TagFilterValueMap{
			"type": filters.FilterValue{Pattern: "a[b"},
		},
		filters.TagFilterValueMap{
			"type": filters.FilterValue{Pattern: "ab{"},
		},
	}
	for _, input := range inputs {
		res, err := fn(input)
		require.Error(t, err)
		require.Nil(t, res)
	}
}

func TestToRunes(t *testing.T) {
	s := "%\n 6s[:\\"
	require.Equal(t, []rune{'%', '\n', ' ', '6', 's', '[', ':', '\\'}, toRunes(s))
}
