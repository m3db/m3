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

package matcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"

	"github.com/stretchr/testify/require"
)

const (
	testRuleSetKey = "testRuleSet"
)

var (
	testNamespace = []byte("testNamespace")
)

func TestRuleSetProperties(t *testing.T) {
	_, _, rs := testRuleSet()
	rs.namespace = testNamespace
	rs.version = 2
	rs.cutoverNanos = 12345
	rs.tombstoned = true

	require.Equal(t, testNamespace, rs.Namespace())
	require.Equal(t, 2, rs.Version())
	require.Equal(t, int64(12345), rs.CutoverNanos())
	require.Equal(t, true, rs.Tombstoned())
}

func TestRuleSetMatchNoMatcher(t *testing.T) {
	_, _, rs := testRuleSet()
	nowNanos := rs.nowFn().UnixNano()
	res, err := rs.ForwardMatch(testID("foo"), nowNanos, nowNanos, rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, rules.EmptyMatchResult, res)
}

func TestRuleSetForwardMatchWithMatcher(t *testing.T) {
	_, _, rs := testRuleSet()
	mockMatcher := &mockMatcher{res: rules.EmptyMatchResult}
	rs.activeSet = mockMatcher

	var (
		now       = rs.nowFn()
		fromNanos = now.Add(-time.Second).UnixNano()
		toNanos   = now.Add(time.Second).UnixNano()
	)

	res, err := rs.ForwardMatch(testID("foo"), fromNanos, toNanos, rules.MatchOptions{})
	require.NoError(t, err)
	require.Equal(t, mockMatcher.res, res)
	require.Equal(t, []byte("foo"), mockMatcher.id)
	require.Equal(t, fromNanos, mockMatcher.fromNanos)
	require.Equal(t, toNanos, mockMatcher.toNanos)
}

func TestToRuleSetNilValue(t *testing.T) {
	_, _, rs := testRuleSet()
	_, err := rs.toRuleSet(nil)
	require.Equal(t, errNilValue, err)
}

func TestToRuleSetUnmarshalError(t *testing.T) {
	_, _, rs := testRuleSet()
	_, err := rs.toRuleSet(&mockValue{})
	require.Error(t, err)
}

func TestToRuleSetSuccess(t *testing.T) {
	store, _, rs := testRuleSet()
	proto := &rulepb.RuleSet{
		Namespace:    string(testNamespace),
		Tombstoned:   false,
		CutoverNanos: 123456,
	}
	_, err := store.SetIfNotExists(testRuleSetKey, proto)
	require.NoError(t, err)
	v, err := store.Get(testRuleSetKey)
	require.NoError(t, err)
	res, err := rs.toRuleSet(v)
	require.NoError(t, err)
	actual := res.(rules.RuleSet)
	require.Equal(t, testNamespace, actual.Namespace())
	require.Equal(t, 1, actual.Version())
	require.Equal(t, int64(123456), actual.CutoverNanos())
	require.Equal(t, false, actual.Tombstoned())
}

func TestRuleSetProcessNamespaceNotRegistered(t *testing.T) {
	var (
		inputs = []rules.RuleSet{
			&mockRuleSet{namespace: "ns1", version: 1, cutoverNanos: 1234, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns2", version: 2, cutoverNanos: 1235, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns3", version: 3, cutoverNanos: 1236, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns4", version: 4, cutoverNanos: 1237, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns5", version: 5, cutoverNanos: 1238, tombstoned: false, matcher: &mockMatcher{}},
		}
	)

	_, cache, rs := testRuleSet()
	memCache := cache.(*memCache)
	for _, input := range inputs {
		err := rs.process(input)
		require.NoError(t, err)
	}

	require.Equal(t, 5, rs.Version())
	require.Equal(t, int64(1238), rs.CutoverNanos())
	require.Equal(t, false, rs.Tombstoned())
	require.NotNil(t, rs.activeSet)
	require.Equal(t, 0, len(memCache.namespaces))
}

func TestRuleSetProcessStaleUpdate(t *testing.T) {
	var (
		inputs = []rules.RuleSet{
			&mockRuleSet{namespace: "ns1", version: 1, cutoverNanos: 1234, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns2", version: 2, cutoverNanos: 1235, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns3", version: 3, cutoverNanos: 1236, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns4", version: 4, cutoverNanos: 1237, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns5", version: 5, cutoverNanos: 1238, tombstoned: false, matcher: &mockMatcher{}},
		}
	)

	_, cache, rs := testRuleSet()
	src := newRuleSet(testNamespace, testNamespacesKey, NewOptions())
	cache.Register([]byte("ns5"), src)

	memCache := cache.(*memCache)
	for _, input := range inputs {
		err := rs.process(input)
		require.NoError(t, err)
	}

	require.Equal(t, 5, rs.Version())
	require.Equal(t, int64(1238), rs.CutoverNanos())
	require.Equal(t, false, rs.Tombstoned())
	require.NotNil(t, rs.activeSet)
	require.Equal(t, 1, len(memCache.namespaces))
	actual := memCache.namespaces["ns5"]
	require.Equal(t, src, actual.source)
}

func TestRuleSetProcessSuccess(t *testing.T) {
	var (
		inputs = []rules.RuleSet{
			&mockRuleSet{namespace: "ns1", version: 1, cutoverNanos: 1234, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns2", version: 2, cutoverNanos: 1235, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns3", version: 3, cutoverNanos: 1236, tombstoned: false, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns4", version: 4, cutoverNanos: 1237, tombstoned: true, matcher: &mockMatcher{}},
			&mockRuleSet{namespace: "ns5", version: 5, cutoverNanos: 1238, tombstoned: false, matcher: &mockMatcher{}},
		}
	)

	_, cache, rs := testRuleSet()
	cache.Register([]byte("ns5"), rs)

	memCache := cache.(*memCache)
	for _, input := range inputs {
		err := rs.process(input)
		require.NoError(t, err)
	}

	require.Equal(t, 5, rs.Version())
	require.Equal(t, int64(1238), rs.CutoverNanos())
	require.Equal(t, false, rs.Tombstoned())
	require.NotNil(t, rs.activeSet)
	require.Equal(t, 1, len(memCache.namespaces))
	actual := memCache.namespaces["ns5"]
	require.Equal(t, rs, actual.source)
}

func testID(id string) id.ID {
	return namespace.NewTestID(id, string(testNamespace))
}

type mockMatcher struct {
	id                             []byte
	fromNanos                      int64
	toNanos                        int64
	res                            rules.MatchResult
	metricType                     metric.Type
	aggregationType                aggregation.Type
	isMultiAggregationTypesAllowed bool
	aggTypesOpts                   aggregation.TypesOptions
}

func (mm *mockMatcher) LatestRollupRules(_ []byte, _ int64) ([]view.RollupRule, error) {
	return []view.RollupRule{}, nil
}

func (mm *mockMatcher) ForwardMatch(
	id id.ID,
	fromNanos, toNanos int64,
	_ rules.MatchOptions,
) (rules.MatchResult, error) {
	mm.id = id.Bytes()
	mm.fromNanos = fromNanos
	mm.toNanos = toNanos
	return mm.res, nil
}

type mockRuleSet struct {
	namespace    string
	version      int
	cutoverNanos int64
	tombstoned   bool
	matcher      *mockMatcher
}

func (r *mockRuleSet) Namespace() []byte                        { return []byte(r.namespace) }
func (r *mockRuleSet) Version() int                             { return r.version }
func (r *mockRuleSet) CutoverNanos() int64                      { return r.cutoverNanos }
func (r *mockRuleSet) LastUpdatedAtNanos() int64                { return 0 }
func (r *mockRuleSet) CreatedAtNanos() int64                    { return 0 }
func (r *mockRuleSet) Tombstoned() bool                         { return r.tombstoned }
func (r *mockRuleSet) Proto() (*rulepb.RuleSet, error)          { return nil, nil }
func (r *mockRuleSet) ActiveSet(_ int64) rules.ActiveSet        { return r.matcher }
func (r *mockRuleSet) ToMutableRuleSet() rules.MutableRuleSet   { return nil }
func (r *mockRuleSet) MappingRules() (view.MappingRules, error) { return nil, nil }
func (r *mockRuleSet) RollupRules() (view.RollupRules, error)   { return nil, nil }
func (r *mockRuleSet) Latest() (view.RuleSet, error)            { return view.RuleSet{}, nil }

func testRuleSet() (kv.Store, cache.Cache, *ruleSet) {
	store := mem.NewStore()
	cache := newMemCache()
	opts := NewOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetRuleSetKeyFn(func(ns []byte) string { return fmt.Sprintf("/rules/%s", ns) }).
		SetOnRuleSetUpdatedFn(func(namespace []byte, ruleSet RuleSet) { cache.Refresh(namespace, ruleSet) }).
		SetMatchRangePast(0)
	return store, cache, newRuleSet(testNamespace, testNamespacesKey, opts).(*ruleSet)
}
