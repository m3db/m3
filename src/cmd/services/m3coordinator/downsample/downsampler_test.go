// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/client"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	dbclient "github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	testAggregationType            = aggregation.Sum
	testAggregationStoragePolicies = []policy.StoragePolicy{
		policy.MustParseStoragePolicy("2s:1d"),
	}
)

const (
	nameTag = "__name__"
)

func TestDownsamplerAggregationWithAutoMappingRulesFromNamespacesWatcher(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	gaugeMetrics, _ := testGaugeMetrics(testGaugeMetricsOptions{})
	require.Equal(t, 1, len(gaugeMetrics))

	gaugeMetric := gaugeMetrics[0]
	numSamples := len(gaugeMetric.samples)

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: gaugeMetric.tags,
					// NB(nate): Automapping rules generated from cluster namespaces currently
					// hardcode 'Last' as the aggregation type. As such, expect value to be the last value
					// in the sample.
					values: []expectedValue{{value: gaugeMetric.samples[numSamples-1]}},
				},
			},
		},
	})

	require.False(t, testDownsampler.downsampler.Enabled())

	origStagedMetadata := originalStagedMetadata(t, testDownsampler)

	session := dbclient.NewMockSession(ctrl)
	setAggregatedNamespaces(t, testDownsampler, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("2s:1d"),
		Resolution:  2 * time.Second,
		Retention:   24 * time.Hour,
		Session:     session,
	})

	waitForStagedMetadataUpdate(t, testDownsampler, origStagedMetadata)

	require.True(t, testDownsampler.downsampler.Enabled())

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationDownsamplesRawMetricWithRollupRule(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag:         "http_requests",
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 42},
			{value: 64, offset: 1 * time.Second},
		},
	}
	res := 1 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.PerSecond,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				// aggregated rollup metric
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					values: []expectedValue{{value: 22}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
				// raw aggregated metric
				{
					tags:   gaugeMetric.tags,
					values: []expectedValue{{value: 42}, {value: 64}},
				},
			},
		},
	})

	// Setup auto-mapping rules.
	require.False(t, testDownsampler.downsampler.Enabled())
	origStagedMetadata := originalStagedMetadata(t, testDownsampler)
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	session := dbclient.NewMockSession(ctrl)
	setAggregatedNamespaces(t, testDownsampler, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("1s:30d"),
		Resolution:  res,
		Retention:   ret,
		Session:     session,
	})
	waitForStagedMetadataUpdate(t, testDownsampler, origStagedMetadata)
	require.True(t, testDownsampler.downsampler.Enabled())

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationToggleEnabled(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{})

	require.False(t, testDownsampler.downsampler.Enabled())

	// Add an aggregated namespace and expect downsampler to be enabled.
	session := dbclient.NewMockSession(ctrl)
	setAggregatedNamespaces(t, testDownsampler, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("2s:1d"),
		Resolution:  2 * time.Second,
		Retention:   24 * time.Hour,
		Session:     session,
	})
	waitForEnabledUpdate(t, &testDownsampler, false)

	require.True(t, testDownsampler.downsampler.Enabled())

	// Set just an unaggregated namespace and expect downsampler to be disabled.
	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("default"),
		Retention:   48 * time.Hour,
		Session:     session,
	})
	require.NoError(t, err)
	require.NoError(t,
		testDownsampler.opts.ClusterNamespacesWatcher.Update(clusters.ClusterNamespaces()))

	waitForEnabledUpdate(t, &testDownsampler, true)

	require.False(t, testDownsampler.downsampler.Enabled())
}

func TestDownsamplerAggregationWithRulesStore(t *testing.T) {
	t.Parallel()

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{})
	rulesStore := testDownsampler.rulesStore

	// Create rules
	nss, err := rulesStore.ReadNamespaces()
	require.NoError(t, err)
	_, err = nss.AddNamespace("default", testUpdateMetadata())
	require.NoError(t, err)

	rule := view.MappingRule{
		ID:              "mappingrule",
		Name:            "mappingrule",
		Filter:          "app:test*",
		AggregationID:   aggregation.MustCompressTypes(testAggregationType),
		StoragePolicies: testAggregationStoragePolicies,
	}

	rs := rules.NewEmptyRuleSet("default", testUpdateMetadata())
	_, err = rs.AddMappingRule(rule, testUpdateMetadata())
	require.NoError(t, err)

	err = rulesStore.WriteAll(nss, rs)
	require.NoError(t, err)

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	// Wait for mapping rule to appear
	logger.Info("waiting for mapping rules to propagate")
	matcher := testDownsampler.matcher
	testMatchID := newTestID(t, map[string]string{
		"__name__": "foo",
		"app":      "test123",
	})
	for {
		now := time.Now().UnixNano()
		res := matcher.ForwardMatch(testMatchID, now, now+1)
		results := res.ForExistingIDAt(now)
		if !results.IsDefault() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRules(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags:   gaugeMetric.tags,
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesPartialReplaceAutoMappingRuleFromNamespacesWatcher(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0, offset: 1 * time.Millisecond},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				// Expect the max to be used and override the default auto
				// mapping rule for the storage policy 2s:24h.
				{
					tags:   gaugeMetric.tags,
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  2 * time.Second,
						Retention:   24 * time.Hour,
					},
				},
				// Expect last to still be used for the storage
				// policy 4s:48h.
				{
					tags: gaugeMetric.tags,
					// NB(nate): Automapping rules generated from cluster namespaces currently
					// hardcode 'Last' as the aggregation type. As such, expect value to be the last value
					// in the sample.
					values: []expectedValue{{value: 0}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  4 * time.Second,
						Retention:   48 * time.Hour,
					},
				},
			},
		},
	})

	origStagedMetadata := originalStagedMetadata(t, testDownsampler)

	session := dbclient.NewMockSession(ctrl)
	setAggregatedNamespaces(t, testDownsampler, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("2s:24h"),
		Resolution:  2 * time.Second,
		Retention:   24 * time.Hour,
		Session:     session,
	}, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("4s:48h"),
		Resolution:  4 * time.Second,
		Retention:   48 * time.Hour,
		Session:     session,
	})

	waitForStagedMetadataUpdate(t, testDownsampler, origStagedMetadata)

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesReplaceAutoMappingRuleFromNamespacesWatcher(t *testing.T) {
	t.Parallel()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				// Expect the max to be used and override the default auto
				// mapping rule for the storage policy 2s:24h.
				{
					tags:   gaugeMetric.tags,
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  2 * time.Second,
						Retention:   24 * time.Hour,
					},
				},
			},
		},
	})

	origStagedMetadata := originalStagedMetadata(t, testDownsampler)

	session := dbclient.NewMockSession(ctrl)
	setAggregatedNamespaces(t, testDownsampler, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("2s:24h"),
		Resolution:  2 * time.Second,
		Retention:   24 * time.Hour,
		Session:     session,
	})

	waitForStagedMetadataUpdate(t, testDownsampler, origStagedMetadata)

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesNoNameTag(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"app":      "nginx_edge",
			"endpoint": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "endpoint",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags:   gaugeMetric.tags,
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesTypeFilter(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"app":      "nginx_edge",
			"endpoint": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "endpoint",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:counter",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
				},
			},
		},
		sampleAppenderOpts: &SampleAppenderOptions{
			MetricType: ts.M3MetricTypeCounter,
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"app":      "nginx_edge",
						"endpoint": "health",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesTypeFilterNoMatch(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"app":      "nginx_edge",
			"endpoint": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "endpoint",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:counter",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
				},
			},
		},
		sampleAppenderOpts: &SampleAppenderOptions{
			MetricType: ts.M3MetricTypeGauge,
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesAggregationType(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"__g0__":               "nginx_edge",
			"__g1__":               "health",
			"__option_id_scheme__": "graphite",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	tags := []Tag{{Name: "__m3_graphite_aggregation__"}}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "__g2__",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:gauge",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"__g0__": "nginx_edge",
						"__g1__": "health",
						"__g2__": "upper",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesMultipleAggregationType(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"__g0__": "nginx_edge",
			"__g1__": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	tags := []Tag{{Name: "__m3_graphite_aggregation__"}}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "__g2__",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:gauge",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
				{
					Filter:       "__m3_type__:gauge",
					Aggregations: []aggregation.Type{aggregation.Sum},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"__g0__": "nginx_edge",
						"__g1__": "health",
						"__g2__": "upper",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
				{
					tags: map[string]string{
						"__g0__": "nginx_edge",
						"__g1__": "health",
						"__g2__": "sum",
					},
					values: []expectedValue{{value: 60}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesGraphitePrefixAndAggregationTags(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"__g0__": "nginx_edge",
			"__g1__": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	tags := []Tag{
		{Name: "__m3_graphite_aggregation__"},
		{Name: "__m3_graphite_prefix__", Value: "stats.counter"},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "__g4__",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:gauge",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"__g0__": "stats",
						"__g1__": "counter",
						"__g2__": "nginx_edge",
						"__g3__": "health",
						"__g4__": "upper",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesGraphitePrefixTag(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"__g0__": "nginx_edge",
			"__g1__": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	tags := []Tag{
		{Name: "__m3_graphite_prefix__", Value: "stats.counter"},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "__g3__",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__m3_type__:gauge",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"__g0__": "stats",
						"__g1__": "counter",
						"__g2__": "nginx_edge",
						"__g3__": "health",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesAugmentTag(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"app":      "nginx_edge",
			"endpoint": "health",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	tags := []Tag{
		{Name: "datacenter", Value: "abc"},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "app",
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 1 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
					Tags: tags,
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						"app":        "nginx_edge",
						"endpoint":   "health",
						"datacenter": "abc",
					},
					values: []expectedValue{{value: 30}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  1 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigRollupRulesNoNameTag(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 42},
			{value: 64, offset: 1 * time.Second},
		},
	}
	res := 1 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		identTag: "endpoint",
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.PerSecond,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigRollupRulesPerSecondSum(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag:         "http_requests",
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 42},
			{value: 64, offset: 1 * time.Second},
		},
	}
	res := 1 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.PerSecond,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					values: []expectedValue{{value: 22}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigRollupRulesIncreaseAdd(t *testing.T) {
	t.Parallel()

	gaugeMetrics := []testGaugeMetric{
		testGaugeMetric{
			tags: map[string]string{
				nameTag:         "http_requests",
				"app":           "nginx_edge",
				"status_code":   "500",
				"endpoint":      "/foo/bar",
				"not_rolled_up": "not_rolled_up_value_1",
			},
			timedSamples: []testGaugeMetricTimedSample{
				{value: 42, offset: 1 * time.Second}, // +42 (should not be accounted since is a reset)
				// Explicit no value.
				{value: 12, offset: 2 * time.Second}, // +12 - simulate a reset (should not be accounted)
				{value: 33, offset: 3 * time.Second}, // +21
			},
		},
		testGaugeMetric{
			tags: map[string]string{
				nameTag:         "http_requests",
				"app":           "nginx_edge",
				"status_code":   "500",
				"endpoint":      "/foo/bar",
				"not_rolled_up": "not_rolled_up_value_2",
			},
			timedSamples: []testGaugeMetricTimedSample{
				{value: 13, offset: 1 * time.Second}, // +13 (should not be accounted since is a reset)
				{value: 27, offset: 2 * time.Second}, // +14
				// Explicit no value.
				{value: 42, offset: 3 * time.Second}, // +15
			},
		},
	}
	res := 1 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.Increase,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.Add,
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					values: []expectedValue{
						{value: 14},
						{value: 50, offset: 1 * time.Second},
					},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigRollupRuleAndDropPolicy(t *testing.T) {
	t.Parallel()

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag:         "http_requests",
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 42},
			{value: 64, offset: 1 * time.Second},
		},
		expectDropPolicyApplied: true,
	}
	res := 1 * time.Second
	ret := 30 * 24 * time.Hour
	filter := fmt.Sprintf("%s:http_requests app:* status_code:* endpoint:*", nameTag)
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter: filter,
					Drop:   true,
				},
			},
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: filter,
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.PerSecond,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					values: []expectedValue{{value: 22}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithTimedSamples(t *testing.T) {
	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{
		timedSamples: true,
	})
	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{
		timedSamples: true,
	})
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		ingest: &testDownsamplerOptionsIngest{
			counterMetrics: counterMetrics,
			gaugeMetrics:   gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: append(counterMetricsExpect, gaugeMetricsExpect...),
		},
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__name__:*",
					Aggregations: []aggregation.Type{testAggregationType},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithOverrideRules(t *testing.T) {
	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{})
	counterMetricsExpect[0].values = []expectedValue{{value: 2}}

	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{})
	gaugeMetricsExpect[0].values = []expectedValue{{value: 5}}

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		sampleAppenderOpts: &SampleAppenderOptions{
			Override: true,
			OverrideRules: SamplesAppenderOverrideRules{
				MappingRules: []AutoMappingRule{
					{
						Aggregations: []aggregation.Type{aggregation.Mean},
						Policies: []policy.StoragePolicy{
							policy.MustParseStoragePolicy("4s:1d"),
						},
					},
				},
			},
		},
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__name__:*",
					Aggregations: []aggregation.Type{testAggregationType},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			counterMetrics: counterMetrics,
			gaugeMetrics:   gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: append(counterMetricsExpect, gaugeMetricsExpect...),
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRemoteAggregatorClient(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Create mock client
	remoteClientMock := client.NewMockClient(ctrl)
	remoteClientMock.EXPECT().Init().Return(nil)

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "__name__:*",
					Aggregations: []aggregation.Type{testAggregationType},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		remoteClientMock: remoteClientMock,
	})

	// Test expected output
	testDownsamplerRemoteAggregation(t, testDownsampler)
}

func TestDownsamplerWithOverrideNamespace(t *testing.T) {
	overrideNamespaceTag := "override_namespace_tag"

	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag:         "http_requests",
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
			// Set namespace tags on ingested metrics.
			// The test demonstrates that overrideNamespaceTag is respected, meaning setting
			// values on defaultNamespaceTag won't affect aggregation.
			defaultNamespaceTag: "namespace_ignored",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 42},
			{value: 64, offset: 5 * time.Second},
		},
	}
	res := 5 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						{
							Transform: &TransformOperationConfiguration{
								Type: transformation.PerSecond,
							},
						},
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		matcherConfig: MatcherConfiguration{NamespaceTag: overrideNamespaceTag},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					values: []expectedValue{{value: 4.4}},
					attributes: &storagemetadata.Attributes{
						MetricsType: storagemetadata.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func originalStagedMetadata(t *testing.T, testDownsampler testDownsampler) []metricpb.StagedMetadatas {
	ds, ok := testDownsampler.downsampler.(*downsampler)
	require.True(t, ok)

	origStagedMetadata := ds.metricsAppenderOpts.defaultStagedMetadatasProtos
	return origStagedMetadata
}

func waitForStagedMetadataUpdate(t *testing.T, testDownsampler testDownsampler, origStagedMetadata []metricpb.StagedMetadatas) {
	ds, ok := testDownsampler.downsampler.(*downsampler)
	require.True(t, ok)

	require.True(t, clock.WaitUntil(func() bool {
		ds.RLock()
		defer ds.RUnlock()

		return !assert.ObjectsAreEqual(origStagedMetadata, ds.metricsAppenderOpts.defaultStagedMetadatasProtos)
	}, time.Second))
}

func waitForEnabledUpdate(t *testing.T, testDownsampler *testDownsampler, current bool) {
	ds, ok := testDownsampler.downsampler.(*downsampler)
	require.True(t, ok)

	require.True(t, clock.WaitUntil(func() bool {
		ds.RLock()
		defer ds.RUnlock()

		return current != ds.enabled
	}, time.Second))
}

type testExpectedWrite struct {
	tags              map[string]string
	values            []expectedValue // use values for multi expected values
	valueAllowedError float64         // use for allowing for slightly inexact values due to timing, etc
	attributes        *storagemetadata.Attributes
}

type expectedValue struct {
	offset time.Duration
	value  float64
}

type testCounterMetric struct {
	tags                    map[string]string
	samples                 []int64
	timedSamples            []testCounterMetricTimedSample
	expectDropPolicyApplied bool
}

type testCounterMetricTimedSample struct {
	time   time.Time
	offset time.Duration
	value  int64
}

type testGaugeMetric struct {
	tags                    map[string]string
	samples                 []float64
	timedSamples            []testGaugeMetricTimedSample
	expectDropPolicyApplied bool
}

type testGaugeMetricTimedSample struct {
	time   time.Time
	offset time.Duration
	value  float64
}

type testCounterMetricsOptions struct {
	timedSamples bool
}

func testCounterMetrics(opts testCounterMetricsOptions) (
	[]testCounterMetric,
	[]testExpectedWrite,
) {
	metric := testCounterMetric{
		tags:    map[string]string{nameTag: "counter0", "app": "testapp", "foo": "bar"},
		samples: []int64{1, 2, 3},
	}
	if opts.timedSamples {
		metric.samples = nil
		metric.timedSamples = []testCounterMetricTimedSample{
			{value: 1}, {value: 2}, {value: 3},
		}
	}
	write := testExpectedWrite{
		tags:   metric.tags,
		values: []expectedValue{{value: 6}},
	}
	return []testCounterMetric{metric}, []testExpectedWrite{write}
}

type testGaugeMetricsOptions struct {
	timedSamples bool
}

func testGaugeMetrics(opts testGaugeMetricsOptions) ([]testGaugeMetric, []testExpectedWrite) {
	metric := testGaugeMetric{
		tags:    map[string]string{nameTag: "gauge0", "app": "testapp", "qux": "qaz"},
		samples: []float64{4, 5, 6},
	}
	if opts.timedSamples {
		metric.samples = nil
		metric.timedSamples = []testGaugeMetricTimedSample{
			{value: 4},
			{value: 5},
			{value: 6, offset: 1 * time.Nanosecond},
		}
	}
	write := testExpectedWrite{
		tags:   metric.tags,
		values: []expectedValue{{value: 15}},
	}
	return []testGaugeMetric{metric}, []testExpectedWrite{write}
}

func testDownsamplerAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	testOpts := testDownsampler.testOpts

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{})
	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{})
	expectedWrites := append(counterMetricsExpect, gaugeMetricsExpect...)

	// Allow overrides
	if ingest := testOpts.ingest; ingest != nil {
		counterMetrics = ingest.counterMetrics
		gaugeMetrics = ingest.gaugeMetrics
	}
	if expect := testOpts.expect; expect != nil {
		expectedWrites = expect.writes
	}

	// Ingest points
	testDownsamplerAggregationIngest(t, testDownsampler,
		counterMetrics, gaugeMetrics)

	// Wait for writes
	logger.Info("wait for test metrics to appear")
	logWritesAccumulated := os.Getenv("TEST_LOG_WRITES_ACCUMULATED") == "true"
	logWritesAccumulatedTicker := time.NewTicker(time.Second)

	logWritesMatch := os.Getenv("TEST_LOG_WRITES_MATCH") == "true"
	logWritesMatchTicker := time.NewTicker(time.Second)

	identTag := nameTag
	if len(testDownsampler.testOpts.identTag) > 0 {
		identTag = testDownsampler.testOpts.identTag
	}

CheckAllWritesArrivedLoop:
	for {
		allWrites := testDownsampler.storage.Writes()
		if logWritesAccumulated {
			select {
			case <-logWritesAccumulatedTicker.C:
				logger.Info("logging accmulated writes",
					zap.Int("numAllWrites", len(allWrites)))
				for _, write := range allWrites {
					logger.Info("accumulated write",
						zap.ByteString("tags", write.Tags().ID()),
						zap.Any("datapoints", write.Datapoints),
						zap.Any("attributes", write.Attributes))
				}
			default:
			}
		}

		for _, expectedWrite := range expectedWrites {
			name := expectedWrite.tags[identTag]
			attrs := expectedWrite.attributes
			writesForNameAndAttrs, _ := findWrites(allWrites, name, identTag, attrs)
			if len(writesForNameAndAttrs) != len(expectedWrite.values) {
				if logWritesMatch {
					select {
					case <-logWritesMatchTicker.C:
						logger.Info("continuing wait for accumulated writes",
							zap.String("name", name),
							zap.Any("attributes", attrs),
							zap.Int("numWritesForNameAndAttrs", len(writesForNameAndAttrs)),
							zap.Int("numExpectedWriteValues", len(expectedWrite.values)),
						)
					default:
					}
				}

				time.Sleep(100 * time.Millisecond)
				continue CheckAllWritesArrivedLoop
			}
		}
		break
	}

	// Verify writes
	logger.Info("verify test metrics")
	allWrites := testDownsampler.storage.Writes()
	if logWritesAccumulated {
		logger.Info("logging accmulated writes to verify",
			zap.Int("numAllWrites", len(allWrites)))
		for _, write := range allWrites {
			logger.Info("accumulated write",
				zap.ByteString("tags", write.Tags().ID()),
				zap.Any("datapoints", write.Datapoints()))
		}
	}

	for _, expectedWrite := range expectedWrites {
		name := expectedWrite.tags[identTag]
		expectedValues := expectedWrite.values
		allowedError := expectedWrite.valueAllowedError

		writesForNameAndAttrs, found := findWrites(allWrites, name, identTag, expectedWrite.attributes)
		require.True(t, found)
		require.Equal(t, len(expectedValues), len(writesForNameAndAttrs))
		for i, expectedValue := range expectedValues {
			write := writesForNameAndAttrs[i]

			assert.Equal(t, expectedWrite.tags, tagsToStringMap(write.Tags()))

			require.Equal(t, 1, len(write.Datapoints()))

			actualValue := write.Datapoints()[0].Value
			if allowedError == 0 {
				// Exact match value.
				assert.Equal(t, expectedValue.value, actualValue)
			} else {
				// Fuzzy match value.
				lower := expectedValue.value - allowedError
				upper := expectedValue.value + allowedError
				withinBounds := (lower <= actualValue) && (actualValue <= upper)
				msg := fmt.Sprintf("expected within: lower=%f, upper=%f, actual=%f",
					lower, upper, actualValue)
				assert.True(t, withinBounds, msg)
			}

			if expectedOffset := expectedValue.offset; expectedOffset > 0 {
				// Check if distance between datapoints as expected (use
				// absolute offset from first write).
				firstTimestamp := writesForNameAndAttrs[0].Datapoints()[0].Timestamp
				actualOffset := write.Datapoints()[0].Timestamp.Sub(firstTimestamp)
				assert.Equal(t, expectedOffset, actualOffset)
			}

			if attrs := expectedWrite.attributes; attrs != nil {
				assert.Equal(t, *attrs, write.Attributes())
			}
		}
	}
}

func testDownsamplerRemoteAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	testOpts := testDownsampler.testOpts

	expectTestCounterMetrics, _ := testCounterMetrics(testCounterMetricsOptions{})
	testCounterMetrics, _ := testCounterMetrics(testCounterMetricsOptions{})

	expectTestGaugeMetrics, _ := testGaugeMetrics(testGaugeMetricsOptions{})
	testGaugeMetrics, _ := testGaugeMetrics(testGaugeMetricsOptions{})

	remoteClientMock := testOpts.remoteClientMock
	require.NotNil(t, remoteClientMock)

	// Expect ingestion
	checkedCounterSamples := 0
	remoteClientMock.EXPECT().
		WriteUntimedCounter(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(counter unaggregated.Counter,
			metadatas metadata.StagedMetadatas,
		) error {
			for _, c := range expectTestCounterMetrics {
				if !strings.Contains(counter.ID.String(), c.tags[nameTag]) {
					continue
				}

				var remainingSamples []int64
				found := false
				for _, s := range c.samples {
					if !found && s == counter.Value {
						found = true
					} else {
						remainingSamples = append(remainingSamples, s)
					}
				}
				c.samples = remainingSamples
				if found {
					checkedCounterSamples++
				}

				break
			}

			return nil
		})

	checkedGaugeSamples := 0
	remoteClientMock.EXPECT().
		WriteUntimedGauge(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(gauge unaggregated.Gauge,
			metadatas metadata.StagedMetadatas,
		) error {
			for _, g := range expectTestGaugeMetrics {
				if !strings.Contains(gauge.ID.String(), g.tags[nameTag]) {
					continue
				}

				var remainingSamples []float64
				found := false
				for _, s := range g.samples {
					if !found && s == gauge.Value {
						found = true
					} else {
						remainingSamples = append(remainingSamples, s)
					}
				}
				g.samples = remainingSamples
				if found {
					checkedGaugeSamples++
				}

				break
			}

			return nil
		})

	// Ingest points
	testDownsamplerAggregationIngest(t, testDownsampler,
		testCounterMetrics, testGaugeMetrics)

	// Ensure we checked counters and gauges
	samplesCounters := 0
	for _, c := range testCounterMetrics {
		samplesCounters += len(c.samples)
	}
	samplesGauges := 0
	for _, c := range testGaugeMetrics {
		samplesGauges += len(c.samples)
	}
	require.Equal(t, samplesCounters, checkedCounterSamples)
	require.Equal(t, samplesGauges, checkedGaugeSamples)
}

func testDownsamplerAggregationIngest(
	t *testing.T,
	testDownsampler testDownsampler,
	testCounterMetrics []testCounterMetric,
	testGaugeMetrics []testGaugeMetric,
) {
	downsampler := testDownsampler.downsampler

	testOpts := testDownsampler.testOpts

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	logger.Info("write test metrics")
	appender, err := downsampler.NewMetricsAppender()
	require.NoError(t, err)
	defer appender.Finalize()

	var opts SampleAppenderOptions
	if testOpts.sampleAppenderOpts != nil {
		opts = *testOpts.sampleAppenderOpts
	}
	// make the current timestamp predictable:
	now := time.Now().Truncate(time.Microsecond)
	for _, metric := range testCounterMetrics {
		appender.NextMetric()

		for name, value := range metric.tags {
			appender.AddTag([]byte(name), []byte(value))
		}

		samplesAppenderResult, err := appender.SamplesAppender(opts)
		require.NoError(t, err)
		require.Equal(t, metric.expectDropPolicyApplied,
			samplesAppenderResult.IsDropPolicyApplied)

		samplesAppender := samplesAppenderResult.SamplesAppender
		for _, sample := range metric.samples {
			err = samplesAppender.AppendCounterSample(sample)
			require.NoError(t, err)
		}
		for _, sample := range metric.timedSamples {
			if sample.time.IsZero() {
				sample.time = now // Allow empty time to mean "now"
			}
			if sample.offset > 0 {
				sample.time = sample.time.Add(sample.offset)
			}
			err = samplesAppender.AppendCounterTimedSample(sample.time, sample.value)
			require.NoError(t, err)
		}
	}
	for _, metric := range testGaugeMetrics {
		appender.NextMetric()

		for name, value := range metric.tags {
			appender.AddTag([]byte(name), []byte(value))
		}

		samplesAppenderResult, err := appender.SamplesAppender(opts)
		require.NoError(t, err)
		require.Equal(t, metric.expectDropPolicyApplied,
			samplesAppenderResult.IsDropPolicyApplied)

		samplesAppender := samplesAppenderResult.SamplesAppender
		for _, sample := range metric.samples {
			err = samplesAppender.AppendGaugeSample(sample)
			require.NoError(t, err)
		}
		for _, sample := range metric.timedSamples {
			if sample.time.IsZero() {
				sample.time = now // Allow empty time to mean "now"
			}
			if sample.offset > 0 {
				sample.time = sample.time.Add(sample.offset)
			}
			err = samplesAppender.AppendGaugeTimedSample(sample.time, sample.value)
			require.NoError(t, err)
		}
	}
}

func setAggregatedNamespaces(
	t *testing.T,
	testDownsampler testDownsampler,
	session dbclient.Session,
	namespaces ...m3.AggregatedClusterNamespaceDefinition,
) {
	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("default"),
		Retention:   48 * time.Hour,
		Session:     session,
	}, namespaces...)
	require.NoError(t, err)
	require.NoError(t, testDownsampler.opts.ClusterNamespacesWatcher.Update(clusters.ClusterNamespaces()))
}

func tagsToStringMap(tags models.Tags) map[string]string {
	stringMap := make(map[string]string, tags.Len())
	for _, t := range tags.Tags {
		stringMap[string(t.Name)] = string(t.Value)
	}

	return stringMap
}

type testDownsampler struct {
	opts           DownsamplerOptions
	testOpts       testDownsamplerOptions
	downsampler    Downsampler
	matcher        matcher.Matcher
	storage        mock.Storage
	rulesStore     rules.Store
	instrumentOpts instrument.Options
}

type testDownsamplerOptions struct {
	clockOpts      clock.Options
	instrumentOpts instrument.Options
	identTag       string

	// Options for the test
	autoMappingRules   []AutoMappingRule
	sampleAppenderOpts *SampleAppenderOptions
	remoteClientMock   *client.MockClient
	rulesConfig        *RulesConfiguration
	matcherConfig      MatcherConfiguration

	// Test ingest and expectations overrides
	ingest *testDownsamplerOptionsIngest
	expect *testDownsamplerOptionsExpect
}

type testDownsamplerOptionsIngest struct {
	counterMetrics []testCounterMetric
	gaugeMetrics   []testGaugeMetric
}

type testDownsamplerOptionsExpect struct {
	writes []testExpectedWrite
}

func newTestDownsampler(t *testing.T, opts testDownsamplerOptions) testDownsampler {
	storage := mock.NewMockStorage()
	rulesKVStore := mem.NewStore()

	clockOpts := clock.NewOptions()
	if opts.clockOpts != nil {
		clockOpts = opts.clockOpts
	}

	// Use a test instrument options by default to get the debug logs on by default.
	instrumentOpts := instrument.NewTestOptions(t)
	if opts.instrumentOpts != nil {
		instrumentOpts = opts.instrumentOpts
	}

	matcherOpts := matcher.NewOptions()

	// Initialize the namespaces
	_, err := rulesKVStore.Set(matcherOpts.NamespacesKey(), &rulepb.Namespaces{})
	require.NoError(t, err)

	rulesetKeyFmt := matcherOpts.RuleSetKeyFn()([]byte("%s"))
	rulesStoreOpts := ruleskv.NewStoreOptions(matcherOpts.NamespacesKey(),
		rulesetKeyFmt, nil)
	rulesStore := ruleskv.NewStore(rulesKVStore, rulesStoreOpts)

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{})
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetSize(2).
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetSize(2).
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))
	metricsAppenderPoolOptions := pool.NewObjectPoolOptions().
		SetSize(2).
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("metrics-appender-pool")))

	cfg := Configuration{
		BufferPastLimits: []BufferPastLimitConfiguration{
			{Resolution: 0, BufferPast: 500 * time.Millisecond},
		},
	}
	if opts.remoteClientMock != nil {
		// Optionally set an override to use remote aggregation
		// with a mock client
		cfg.RemoteAggregator = &RemoteAggregatorConfiguration{
			clientOverride: opts.remoteClientMock,
		}
	}
	if opts.rulesConfig != nil {
		cfg.Rules = opts.rulesConfig
	}
	cfg.Matcher = opts.matcherConfig

	instance, err := cfg.NewDownsampler(DownsamplerOptions{
		Storage:                    storage,
		ClusterClient:              clusterclient.NewMockClient(gomock.NewController(t)),
		RulesKVStore:               rulesKVStore,
		ClusterNamespacesWatcher:   m3.NewClusterNamespacesWatcher(),
		ClockOptions:               clockOpts,
		InstrumentOptions:          instrumentOpts,
		TagEncoderOptions:          tagEncoderOptions,
		TagDecoderOptions:          tagDecoderOptions,
		TagEncoderPoolOptions:      tagEncoderPoolOptions,
		TagDecoderPoolOptions:      tagDecoderPoolOptions,
		MetricsAppenderPoolOptions: metricsAppenderPoolOptions,
		RWOptions:                  xio.NewOptions(),
		TagOptions:                 models.NewTagOptions(),
	})
	require.NoError(t, err)

	downcast, ok := instance.(*downsampler)
	require.True(t, ok)

	return testDownsampler{
		opts:           downcast.opts,
		testOpts:       opts,
		downsampler:    instance,
		matcher:        downcast.agg.matcher,
		storage:        storage,
		rulesStore:     rulesStore,
		instrumentOpts: instrumentOpts,
	}
}

func newTestID(t *testing.T, tags map[string]string) id.ID {
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()

	tagsIter := newTags()
	for name, value := range tags {
		tagsIter.append([]byte(name), []byte(value))
	}

	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(tagsIter)
	require.NoError(t, err)

	data, ok := tagEncoder.Data()
	require.True(t, ok)

	size := 1
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
			CheckBytesWrapperPoolSize: &size,
		}),
		pool.NewObjectPoolOptions().SetSize(size))
	tagDecoderPool.Init()

	tagDecoder := tagDecoderPool.Get()

	iter := serialize.NewMetricTagsIterator(tagDecoder, nil)
	iter.Reset(data.Bytes())
	return iter
}

func findWrites(
	writes []*storage.WriteQuery,
	name, identTag string,
	optionalMatchAttrs *storagemetadata.Attributes,
) ([]*storage.WriteQuery, bool) {
	var results []*storage.WriteQuery
	for _, w := range writes {
		if t, ok := w.Tags().Get([]byte(identTag)); ok {
			if !bytes.Equal(t, []byte(name)) {
				// Does not match name.
				continue
			}
			if optionalMatchAttrs != nil && w.Attributes() != *optionalMatchAttrs {
				// Tried to match attributes and not matched.
				continue
			}

			// Matches name and all optional lookups.
			results = append(results, w)
		}
	}
	return results, len(results) > 0
}

func testUpdateMetadata() rules.UpdateMetadata {
	return rules.NewRuleSetUpdateHelper(0).NewUpdateMetadata(time.Now().UnixNano(), "test")
}
