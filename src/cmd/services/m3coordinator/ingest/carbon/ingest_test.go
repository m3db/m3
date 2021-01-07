// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingestcarbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Keep this value large enough to catch issues like the ingester
	// not copying the name.
	numLinesInTestPacket = 10000
)

var (
	// Created by init().
	testMetrics = []testMetric{}
	testPacket  = []byte{}

	testOptions = Options{
		InstrumentOptions: instrument.NewOptions(),
		WorkerPool:        nil, // Set by init().
	}

	testTagOpts = models.NewTagOptions().
			SetIDSchemeType(models.TypeGraphite)

	testRulesMatchAll = CarbonIngesterRules{
		Rules: []config.CarbonIngesterRuleConfiguration{
			{
				Pattern: ".*", // Match all.
				Aggregation: config.CarbonIngesterAggregationConfiguration{
					Enabled: truePtr,
					Type:    aggregateMeanPtr,
				},
				Policies: []config.CarbonIngesterStoragePolicyConfiguration{
					{
						Resolution: 10 * time.Second,
						Retention:  48 * time.Hour,
					},
				},
			},
		},
	}
)

type testRulesOptions struct {
	substring string
	prefix    string
	suffix    string
}

func testRules(opts testRulesOptions) CarbonIngesterRules {
	// Match prefix + substring + "1" + suffix twice with two patterns, and
	// in one case with two policies and in the second with one policy. In
	// addition, also match prefix + substring + "2" + suffix wit a single
	// pattern and policy.
	return CarbonIngesterRules{
		Rules: []config.CarbonIngesterRuleConfiguration{
			{
				Pattern: opts.prefix + opts.substring + "1" + opts.suffix,
				Aggregation: config.CarbonIngesterAggregationConfiguration{
					Enabled: truePtr,
					Type:    aggregateMeanPtr,
				},
				Policies: []config.CarbonIngesterStoragePolicyConfiguration{
					{
						Resolution: 10 * time.Second,
						Retention:  48 * time.Hour,
					},
					{
						Resolution: 1 * time.Hour,
						Retention:  7 * 24 * time.Hour,
					},
				},
			},
			// Should never match as the previous one takes precedence.
			{
				Pattern: opts.prefix + opts.substring + "1" + opts.suffix,
				Aggregation: config.CarbonIngesterAggregationConfiguration{
					Enabled: truePtr,
					Type:    aggregateMeanPtr,
				},
				Policies: []config.CarbonIngesterStoragePolicyConfiguration{
					{
						Resolution: time.Minute,
						Retention:  24 * time.Hour,
					},
				},
			},
			{
				Pattern: opts.prefix + opts.substring + "2" + opts.suffix,
				Aggregation: config.CarbonIngesterAggregationConfiguration{
					Enabled: truePtr,
					Type:    aggregateLastPtr,
				},
				Policies: []config.CarbonIngesterStoragePolicyConfiguration{
					{
						Resolution: 10 * time.Second,
						Retention:  48 * time.Hour,
					},
				},
			},
			{
				Pattern: opts.prefix + opts.substring + "3" + opts.suffix,
				Aggregation: config.CarbonIngesterAggregationConfiguration{
					Enabled: falsePtr,
				},
				Policies: []config.CarbonIngesterStoragePolicyConfiguration{
					{
						Resolution: 1 * time.Hour,
						Retention:  7 * 24 * time.Hour,
					},
				},
			},
		},
	}
}

func testExpectedWriteOptions(substring string) map[string]ingest.WriteOptions {
	// Maps the rules above to their expected write options.
	return map[string]ingest.WriteOptions{
		substring + "1": {
			DownsampleOverride: true,
			DownsampleMappingRules: []downsample.AutoMappingRule{
				{
					Aggregations: []aggregation.Type{aggregation.Mean},
					Policies: []policy.StoragePolicy{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
						policy.NewStoragePolicy(1*time.Hour, xtime.Second, 7*24*time.Hour),
					},
				},
			},
			WriteOverride: true,
		},
		substring + "2": {
			DownsampleOverride: true,
			DownsampleMappingRules: []downsample.AutoMappingRule{
				{
					Aggregations: []aggregation.Type{aggregation.Last},
					Policies:     []policy.StoragePolicy{policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour)},
				},
			},
			WriteOverride: true,
		},
		substring + "3": {
			DownsampleOverride: true,
			WriteOverride:      true,
			WriteStoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Hour, xtime.Second, 7*24*time.Hour),
			},
		},
	}
}

func TestIngesterHandleConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

	var (
		lock = sync.Mutex{}

		found = []testMetric{}
		idx   = 0
	)
	mockDownsamplerAndWriter.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), xtime.Second, gomock.Any(), gomock.Any()).DoAndReturn(func(
		_ context.Context,
		tags models.Tags,
		dp ts.Datapoints,
		unit xtime.Unit,
		annotation []byte,
		overrides ingest.WriteOptions,
	) interface{} {
		lock.Lock()
		// Clone tags because they (and their underlying bytes) are pooled.
		found = append(found, testMetric{
			tags:      tags.Clone(),
			timestamp: int(dp[0].Timestamp.Unix()),
			value:     dp[0].Value,
		})

		// Make 1 in 10 writes fail to test those paths.
		returnErr := idx%10 == 0
		idx++
		lock.Unlock()

		if returnErr {
			return errors.New("some_error")
		}
		return nil
	}).AnyTimes()

	session := client.NewMockSession(ctrl)
	watcher := newTestWatcher(t, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("10s:48h"),
		Resolution:  10 * time.Second,
		Retention:   48 * time.Hour,
		Session:     session,
	})

	byteConn := &byteConn{b: bytes.NewBuffer(testPacket)}
	ingester, err := NewIngester(mockDownsamplerAndWriter, watcher, newTestOpts(testRulesMatchAll))
	require.NoError(t, err)
	ingester.Handle(byteConn)

	assertTestMetricsAreEqual(t, testMetrics, found)
}

func TestIngesterHonorsMatchers(t *testing.T) {
	tests := []struct {
		name                 string
		input                string
		rules                CarbonIngesterRules
		expectedWriteOptions map[string]ingest.WriteOptions
		expectedMetrics      []testMetric
	}{
		{
			name: "regexp matching",
			input: "foo.match-regex1.bar.baz 1 1\n" +
				"foo.match-regex2.bar.baz 2 2\n" +
				"foo.match-regex3.bar.baz 3 3\n" +
				"foo.match-not-regex.bar.baz 4 4",
			rules: testRules(testRulesOptions{
				substring: "match-regex",
				prefix:    ".*",
				suffix:    ".*",
			}),
			expectedWriteOptions: testExpectedWriteOptions("match-regex"),
			expectedMetrics: []testMetric{
				{
					metric:    []byte("foo.match-regex1.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-regex1.bar.baz")),
					timestamp: 1,
					value:     1,
				},
				{
					metric:    []byte("foo.match-regex2.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-regex2.bar.baz")),
					timestamp: 2,
					value:     2,
				},
				{
					metric:    []byte("foo.match-regex3.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-regex3.bar.baz")),
					timestamp: 3,
					value:     3,
				},
			},
		},
		{
			name: "contains matching",
			input: "foo.match-contains1.bar.baz 1 1\n" +
				"foo.match-contains2.bar.baz 2 2\n" +
				"foo.match-contains3.bar.baz 3 3\n" +
				"foo.match-not-contains.bar.baz 4 4",
			rules: testRules(testRulesOptions{
				substring: "match-contains",
				prefix:    ".*",
				suffix:    ".*",
			}),
			expectedWriteOptions: testExpectedWriteOptions("match-contains"),
			expectedMetrics: []testMetric{
				{
					metric:    []byte("foo.match-contains1.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-contains1.bar.baz")),
					timestamp: 1,
					value:     1,
				},
				{
					metric:    []byte("foo.match-contains2.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-contains2.bar.baz")),
					timestamp: 2,
					value:     2,
				},
				{
					metric:    []byte("foo.match-contains3.bar.baz"),
					tags:      mustGenerateTagsFromName(t, []byte("foo.match-contains3.bar.baz")),
					timestamp: 3,
					value:     3,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

			var (
				lock  = sync.Mutex{}
				found = []testMetric{}
			)
			mockDownsamplerAndWriter.EXPECT().
				Write(gomock.Any(), gomock.Any(), gomock.Any(), xtime.Second, gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					tags models.Tags,
					dp ts.Datapoints,
					unit xtime.Unit,
					annotation []byte,
					writeOpts ingest.WriteOptions,
				) interface{} {
					lock.Lock()
					// Clone tags because they (and their underlying bytes) are pooled.
					found = append(found, testMetric{
						tags:      tags.Clone(),
						timestamp: int(dp[0].Timestamp.Unix()),
						value:     dp[0].Value,
					})
					lock.Unlock()

					// Use panic's instead of require/assert because those don't behave properly when the assertion
					// is run in a background goroutine. Also we match on the second tag val just due to the nature
					// of how the patterns were written.
					secondTagVal := string(tags.Tags[1].Value)
					expectedWriteOpts, ok := test.expectedWriteOptions[secondTagVal]
					if !ok {
						panic(fmt.Sprintf("expected write options for: %s", secondTagVal))
					}

					if !reflect.DeepEqual(expectedWriteOpts, writeOpts) {
						panic(fmt.Sprintf("expected %v to equal %v for metric: %s",
							expectedWriteOpts, writeOpts, secondTagVal))
					}

					return nil
				}).
				AnyTimes()

			byteConn := &byteConn{b: bytes.NewBuffer([]byte(test.input))}

			session := client.NewMockSession(ctrl)
			watcher := newTestWatcher(t, session, m3.AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID("10s:48h"),
				Resolution:  10 * time.Second,
				Retention:   48 * time.Hour,
				Session:     session,
			}, m3.AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID("1m:24h"),
				Resolution:  1 * time.Minute,
				Retention:   24 * time.Hour,
				Session:     session,
			}, m3.AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID("1h:168h"),
				Resolution:  1 * time.Hour,
				Retention:   168 * time.Hour,
				Session:     session,
			})

			ingester, err := NewIngester(mockDownsamplerAndWriter, watcher,
				newTestOpts(test.rules))
			require.NoError(t, err)
			ingester.Handle(byteConn)

			assertTestMetricsAreEqual(t, test.expectedMetrics, found)
		})
	}
}

func TestIngesterNoStaticRules(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var expectationErr error
	mockDownsamplerAndWriter, found := newMockDownsamplerAndWriter(ctrl, func(mappingRules []downsample.AutoMappingRule) {
		if len(mappingRules) != 1 {
			expectationErr = errors.New(fmt.Sprintf("expected: len(DownsampleMappingRules) == 1, got: %v", len(mappingRules)))
		}
		policies := mappingRules[0].Policies

		if len(policies) != 1 {
			panic(fmt.Sprintf("expected: len(policies) == 1, got: %v", len(policies)))
		}
		expectedPolicy := policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour)
		if ok := expectedPolicy == policies[0]; !ok {
			expectationErr = errors.New(fmt.Sprintf("expected storage policy: %+v, got: %+v", expectedPolicy, policies[0]))
		}
	})

	session := client.NewMockSession(ctrl)
	watcher := newTestWatcher(t, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("10s:48h"),
		Resolution:  10 * time.Second,
		Retention:   48 * time.Hour,
		Session:     session,
	})

	conn := &byteConn{b: bytes.NewBuffer(testPacket)}
	i, err := NewIngester(mockDownsamplerAndWriter, watcher, newTestOpts(CarbonIngesterRules{Rules: nil}))
	require.NoError(t, err)

	downcast, ok := i.(*ingester)
	require.True(t, ok)

	// Wait until rules are updated and store them for later comparison.
	var origRules []ruleAndMatcher
	require.True(t, clock.WaitUntil(func() bool {
		downcast.RLock()
		origRules = downcast.rules
		downcast.RUnlock()

		return len(origRules) > 0
	}, time.Second))

	i.Handle(conn)

	assertTestMetricsAreEqual(t, testMetrics, *found)
	require.NoError(t, expectationErr)

	// Simulate namespace changes while ingester exists.
	clusterNamespaces := newClusterNamespaces(t, session, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("10s:48h"),
		Resolution:  10 * time.Second,
		Retention:   48 * time.Hour,
		Session:     session,
	}, m3.AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("1m:7d"),
		Resolution:  1 * time.Minute,
		Retention:   168 * time.Hour,
		Session:     session,
	})

	err = watcher.Update(clusterNamespaces)
	require.NoError(t, err)

	// Ensure storage policies on mapping rules have been updated now that new aggregated namespaces have
	// been added.
	expectationErr = nil
	mockDownsamplerAndWriter, found = newMockDownsamplerAndWriter(ctrl, func(mappingRules []downsample.AutoMappingRule) {
		// Use panics instead of require/assert because those don't behave properly when the assertion
		// is run in a background goroutine.
		if len(mappingRules) != 1 {
			panic(fmt.Sprintf("expected: len(DownsampleMappingRules) == 1, got: %v", len(mappingRules)))
		}
		policies := mappingRules[0].Policies

		if len(policies) != 2 {
			panic(fmt.Sprintf("expected: len(policies) == 2, got: %v", len(policies)))
		}
		expectedPolicy := policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour)
		if ok := expectedPolicy == policies[0]; !ok {
			expectationErr = errors.New(fmt.Sprintf("expected storage policy: %+v, got: %+v", expectedPolicy, policies[0]))
		}
		expectedPolicy = policy.NewStoragePolicy(1*time.Minute, xtime.Second, 168*time.Hour)
		if ok := expectedPolicy == policies[1]; !ok {
			expectationErr = errors.New(fmt.Sprintf("expected storage policy: %+v, got: %+v", expectedPolicy, policies[1]))
		}
	})

	// Need to do this to update the mock to check for storage policy updates we expect to see.
	downcast.downsamplerAndWriter = mockDownsamplerAndWriter

	// Wait for rules to be updated again.
	require.True(t, clock.WaitUntil(func() bool {
		downcast.RLock()
		defer downcast.RUnlock()

		return !assert.ObjectsAreEqual(origRules, downcast.rules)
	}, time.Second))

	conn = &byteConn{b: bytes.NewBuffer(testPacket)}
	downcast.Handle(conn)

	assertTestMetricsAreEqual(t, testMetrics, *found)
	require.NoError(t, expectationErr)
}

func newMockDownsamplerAndWriter(
	ctrl *gomock.Controller,
	expectations func(mappingRules []downsample.AutoMappingRule),
) (*ingest.MockDownsamplerAndWriter, *[]testMetric) {
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

	var (
		lock    sync.Mutex
		metrics = make([]testMetric, 0, numLinesInTestPacket)
		found   = &metrics
		idx     = 0
	)
	mockDownsamplerAndWriter.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), xtime.Second, gomock.Any(), gomock.Any()).DoAndReturn(func(
		_ context.Context,
		tags models.Tags,
		dp ts.Datapoints,
		unit xtime.Unit,
		annotation []byte,
		writeOpts ingest.WriteOptions,
	) interface{} {
		lock.Lock()
		// Clone tags because they (and their underlying bytes) are pooled.
		*found = append(*found, testMetric{
			tags:      tags.Clone(),
			timestamp: int(dp[0].Timestamp.Unix()),
			value:     dp[0].Value,
		})

		// Make 1 in 10 writes fail to test those paths.
		returnErr := idx%10 == 0
		idx++
		lock.Unlock()

		expectations(writeOpts.DownsampleMappingRules)

		if returnErr {
			return errors.New("some_error")
		}
		return nil
	}).AnyTimes()

	return mockDownsamplerAndWriter, found
}

func TestGenerateTagsFromName(t *testing.T) {
	testCases := []struct {
		name         string
		id           string
		expectedTags []models.Tag
		expectedErr  error
	}{
		{
			name: "foo",
			id:   "foo",
			expectedTags: []models.Tag{
				{Name: graphite.TagName(0), Value: []byte("foo")},
			},
		},
		{
			name: "foo.bar.baz",
			id:   "foo.bar.baz",
			expectedTags: []models.Tag{
				{Name: graphite.TagName(0), Value: []byte("foo")},
				{Name: graphite.TagName(1), Value: []byte("bar")},
				{Name: graphite.TagName(2), Value: []byte("baz")},
			},
		},
		{
			name: "foo.bar.baz.",
			id:   "foo.bar.baz",
			expectedTags: []models.Tag{
				{Name: graphite.TagName(0), Value: []byte("foo")},
				{Name: graphite.TagName(1), Value: []byte("bar")},
				{Name: graphite.TagName(2), Value: []byte("baz")},
			},
		},
		{
			name:         "foo..bar..baz..",
			expectedErr:  fmt.Errorf("carbon metric: foo..bar..baz.. has duplicate separator"),
			expectedTags: []models.Tag{},
		},
		{
			name:         "foo.bar.baz..",
			expectedErr:  fmt.Errorf("carbon metric: foo.bar.baz.. has duplicate separator"),
			expectedTags: []models.Tag{},
		},
	}

	opts := models.NewTagOptions().SetIDSchemeType(models.TypeGraphite)
	for _, tc := range testCases {
		tags, err := GenerateTagsFromName([]byte(tc.name), opts)
		if tc.expectedErr != nil {
			require.Equal(t, tc.expectedErr, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, []byte(tc.id), tags.ID())
		}
		require.Equal(t, tc.expectedTags, tags.Tags)
	}
}

func newTestOpts(rules CarbonIngesterRules) Options {
	cfg := config.CarbonIngesterConfiguration{Rules: rules.Rules}
	opts := testOptions
	opts.IngesterConfig = cfg
	return opts
}

func newTestWatcher(
	t *testing.T,
	session client.Session,
	aggNamespaces ...m3.AggregatedClusterNamespaceDefinition,
) m3.ClusterNamespacesWatcher {
	clusterNamespaces := newClusterNamespaces(t, session, aggNamespaces...)
	watcher := m3.NewClusterNamespacesWatcher()
	err := watcher.Update(clusterNamespaces)
	require.NoError(t, err)

	return watcher
}

func newClusterNamespaces(
	t *testing.T,
	session client.Session,
	aggNamespaces ...m3.AggregatedClusterNamespaceDefinition,
) m3.ClusterNamespaces {
	clusters, err := m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("default"),
		Retention:   48 * time.Hour,
		Session:     session,
	}, aggNamespaces...)
	require.NoError(t, err)

	return clusters.ClusterNamespaces()
}

// byteConn implements the net.Conn interface so that we can test the handler without
// going over the network.
type byteConn struct {
	b      io.Reader
	closed bool
}

func (b *byteConn) Read(buf []byte) (n int, err error) {
	if !b.closed {
		return b.b.Read(buf)
	}

	return 0, io.EOF
}

func (b *byteConn) Write(buf []byte) (n int, err error) {
	panic("not_implemented")
}

func (b *byteConn) Close() error {
	b.closed = true
	return nil
}

func (b *byteConn) LocalAddr() net.Addr {
	panic("not_implemented")
}

func (b *byteConn) RemoteAddr() net.Addr {
	panic("not_implemented")
}

func (b *byteConn) SetDeadline(t time.Time) error {
	panic("not_implemented")
}

func (b *byteConn) SetReadDeadline(t time.Time) error {
	panic("not_implemented")
}

func (b *byteConn) SetWriteDeadline(t time.Time) error {
	panic("not_implemented")
}

type testMetric struct {
	metric    []byte
	tags      models.Tags
	timestamp int
	value     float64
}

func assertTestMetricsAreEqual(t *testing.T, a, b []testMetric) {
	require.Equal(t, len(a), len(b))

	sort.Slice(b, func(i, j int) bool {
		return b[i].timestamp < b[j].timestamp
	})

	for i, f := range b {
		require.Equal(t, a[i].tags, f.tags)
		require.Equal(t, a[i].timestamp, f.timestamp)
		require.Equal(t, a[i].value, f.value)
	}
}

func init() {
	var err error
	testOptions.WorkerPool, err = xsync.NewPooledWorkerPool(16, xsync.NewPooledWorkerPoolOptions())
	if err != nil {
		panic(err)
	}
	testOptions.WorkerPool.Init()

	for i := 0; i < numLinesInTestPacket; i++ {
		var metric []byte

		if i%10 == 0 {
			// Make 1 in 10 lines invalid to test the error paths.
			if rand.Intn(2) == 0 {
				// Invalid line altogether.
				line := []byte(fmt.Sprintf("garbage line %d \n", i))
				testPacket = append(testPacket, line...)
				continue
			} else {
				// Valid line, but invalid name (too many separators).
				line := []byte(fmt.Sprintf("test..metric..%d %d %d\n", i, i, i))
				testPacket = append(testPacket, line...)
				continue
			}
		}

		metric = []byte(fmt.Sprintf("test.metric.%d", i))

		opts := models.NewTagOptions().SetIDSchemeType(models.TypeGraphite)
		tags, err := GenerateTagsFromName(metric, opts)
		if err != nil {
			panic(err)
		}
		testMetrics = append(testMetrics, testMetric{
			metric:    metric,
			tags:      tags,
			timestamp: i,
			value:     float64(i),
		})

		line := []byte(fmt.Sprintf("%s %d %d\n", string(metric), i, i))
		testPacket = append(testPacket, line...)
	}
}

func mustGenerateTagsFromName(t *testing.T, name []byte) models.Tags {
	tags, err := GenerateTagsFromName(name, testTagOpts)
	require.NoError(t, err)
	return tags
}

var (
	// Boilerplate to deal with optional config value nonsense.
	trueVar          = true
	truePtr          = &trueVar
	falseVar         = false
	falsePtr         = &falseVar
	aggregateMean    = aggregation.Mean
	aggregateLast    = aggregation.Last
	aggregateMeanPtr = &aggregateMean
	aggregateLastPtr = &aggregateLast
)
