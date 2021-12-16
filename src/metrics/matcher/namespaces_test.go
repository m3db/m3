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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const (
	testNamespacesKey = "testNamespaces"
)

func TestNamespacesWatchAndClose(t *testing.T) {
	store, _, nss, _ := testNamespaces()
	proto := &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        true,
					},
				},
			},
		},
	}
	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)
	require.NoError(t, nss.Watch())
	require.Equal(t, 1, nss.rules.Len())
	nss.Close()
}

func TestNamespacesWatchSoftErr(t *testing.T) {
	_, _, nss, _ := testNamespaces()
	// No value set, so this will soft error
	require.NoError(t, nss.Open())
}

func TestNamespacesWatchRulesetSoftErr(t *testing.T) {
	store, _, nss, _ := testNamespaces()
	proto := &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        true,
					},
				},
			},
		},
	}
	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)

	// This should also soft error even though the underlying ruleset does not exist
	require.NoError(t, nss.Open())
}

func TestNamespacesWatchHardErr(t *testing.T) {
	_, _, _, opts := testNamespaces()
	opts = opts.SetRequireNamespaceWatchOnInit(true)
	nss := NewNamespaces(testNamespacesKey, opts).(*namespaces)
	// This should hard error with RequireNamespaceWatchOnInit enabled
	require.Error(t, nss.Open())
}

func TestNamespacesWatchRulesetHardErr(t *testing.T) {
	store, _, _, opts := testNamespaces()
	opts = opts.SetRequireNamespaceWatchOnInit(true)
	nss := NewNamespaces(testNamespacesKey, opts).(*namespaces)

	proto := &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        true,
					},
				},
			},
		},
	}
	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)

	// This should also hard error with RequireNamespaceWatchOnInit enabled,
	// because the underlying ruleset does not exist
	require.Error(t, nss.Open())
}

func TestNamespacesOpenWithInterrupt(t *testing.T) {
	interruptedCh := make(chan struct{}, 1)
	interruptedCh <- struct{}{}

	_, _, nss, _ := testNamespacesWithInterruptedCh(interruptedCh)
	err := nss.Open()

	require.Error(t, err)
	require.Equal(t, err.Error(), "interrupted")
}

func TestToNamespacesNilValue(t *testing.T) {
	_, _, nss, _ := testNamespaces()
	_, err := nss.toNamespaces(nil)
	require.Equal(t, errNilValue, err)
}

func TestToNamespacesUnmarshalError(t *testing.T) {
	_, _, nss, _ := testNamespaces()
	_, err := nss.toNamespaces(&mockValue{})
	require.Error(t, err)
}

func TestToNamespacesSuccess(t *testing.T) {
	store, _, nss, _ := testNamespaces()
	proto := &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        true,
					},
				},
			},
		},
	}
	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)
	v, err := store.Get(testNamespacesKey)
	require.NoError(t, err)
	res, err := nss.toNamespaces(v)
	require.NoError(t, err)
	actual := res.(rules.Namespaces)
	require.Equal(t, 1, actual.Version())
	require.Equal(t, 1, len(actual.Namespaces()))
	require.Equal(t, []byte("fooNs"), actual.Namespaces()[0].Name())
}

func TestNamespacesProcess(t *testing.T) {
	_, cache, nss, opts := testNamespaces()
	c := cache.(*memCache)

	for _, input := range []struct {
		namespace  []byte
		id         string
		version    int
		tombstoned bool
	}{
		{namespace: []byte("fooNs"), id: "foo", version: 1, tombstoned: false},
		{namespace: []byte("barNs"), id: "bar", version: 1, tombstoned: true},
		{namespace: []byte("catNs"), id: "cat", version: 3, tombstoned: true},
		{namespace: []byte("lolNs"), id: "lol", version: 3, tombstoned: true},
	} {
		rs := newRuleSet(input.namespace, input.id, opts).(*ruleSet)
		rs.Value = &mockRuntimeValue{key: input.id}
		rs.version = input.version
		rs.tombstoned = input.tombstoned
		nss.rules.Set(input.namespace, rs)
		c.namespaces[string(input.namespace)] = memResults{source: rs}
	}

	update := &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			{
				Name: "barNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
			{
				Name: "bazNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			{
				Name: "catNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 3,
						Tombstoned:        true,
					},
				},
			},
			{
				Name:      "mehNs",
				Snapshots: nil,
			},
		},
	}

	nssValue, err := rules.NewNamespaces(5, update)
	require.NoError(t, err)
	require.NoError(t, nss.process(nssValue))
	require.Equal(t, 5, nss.rules.Len())
	require.Equal(t, 5, len(c.namespaces))

	expected := []struct {
		key       string
		watched   int32
		unwatched int32
	}{
		{key: "foo", watched: 1, unwatched: 0},
		{key: "bar", watched: 1, unwatched: 0},
		{key: "/ruleset/bazNs", watched: 1, unwatched: 0},
		{key: "cat", watched: 0, unwatched: 1},
		{key: "/ruleset/mehNs", watched: 1, unwatched: 0},
	}
	for i, ns := range update.Namespaces {
		rs, exists := nss.rules.Get([]byte(ns.Name))
		ruleSet := rs.(*ruleSet)
		require.True(t, exists)
		require.Equal(t, expected[i].key, rs.Key())
		require.Equal(t, ruleSet, c.namespaces[string(ns.Name)].source)
		mv, ok := ruleSet.Value.(*mockRuntimeValue)
		if !ok {
			continue
		}
		require.Equal(t, expected[i].watched, mv.numWatched)
		require.Equal(t, expected[i].unwatched, mv.numUnwatched)
	}
}

func testNamespacesWithInterruptedCh(interruptedCh chan struct{}) (kv.TxnStore, cache.Cache, *namespaces, Options) {
	store := mem.NewStore()
	cache := newMemCache()
	opts := NewOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetNamespacesKey(testNamespacesKey).
		SetMatchRangePast(0).
		SetOnNamespaceAddedFn(func(namespace []byte, ruleSet RuleSet) {
			cache.Register(namespace, ruleSet)
		}).
		SetOnNamespaceRemovedFn(func(namespace []byte) {
			cache.Unregister(namespace)
		}).
		SetOnRuleSetUpdatedFn(func(namespace []byte, ruleSet RuleSet) {
			cache.Register(namespace, ruleSet)
		}).
		SetInterruptedCh(interruptedCh)

	return store, cache, NewNamespaces(testNamespacesKey, opts).(*namespaces), opts
}

func testNamespaces() (kv.TxnStore, cache.Cache, *namespaces, Options) {
	return testNamespacesWithInterruptedCh(nil)
}

type memResults struct {
	results map[string]rules.MatchResult
	source  rules.Matcher
}

type memCache struct {
	sync.RWMutex
	nsResolver namespace.Resolver
	namespaces map[string]memResults
}

func newMemCache() cache.Cache {
	return &memCache{namespaces: make(map[string]memResults), nsResolver: namespace.Default}
}

func (c *memCache) ForwardMatch(id id.ID, _, _ int64, _ rules.MatchOptions) (rules.MatchResult, error) {
	c.RLock()
	defer c.RUnlock()
	if results, exists := c.namespaces[string(c.nsResolver.Resolve(id))]; exists {
		return results.results[string(id.Bytes())], nil
	}
	return rules.EmptyMatchResult, nil
}

func (c *memCache) Register(namespace []byte, source rules.Matcher) {
	c.Lock()
	defer c.Unlock()

	results, exists := c.namespaces[string(namespace)]
	if !exists {
		results = memResults{
			results: make(map[string]rules.MatchResult),
			source:  source,
		}
		c.namespaces[string(namespace)] = results
		return
	}
	panic(fmt.Errorf("re-registering existing namespace %s", namespace))
}

func (c *memCache) Refresh(namespace []byte, source rules.Matcher) {
	c.Lock()
	defer c.Unlock()

	results, exists := c.namespaces[string(namespace)]
	if !exists || results.source != source {
		return
	}
	c.namespaces[string(namespace)] = memResults{
		results: make(map[string]rules.MatchResult),
		source:  source,
	}
}

func (c *memCache) Unregister(namespace []byte) {
	c.Lock()
	defer c.Unlock()
	delete(c.namespaces, string(namespace))
}

func (c *memCache) Close() error { return nil }

type mockRuntimeValue struct {
	key          string
	numWatched   int32
	numUnwatched int32
}

func (mv *mockRuntimeValue) Key() string  { return mv.key }
func (mv *mockRuntimeValue) Watch() error { atomic.AddInt32(&mv.numWatched, 1); return nil }
func (mv *mockRuntimeValue) Unwatch()     { atomic.AddInt32(&mv.numUnwatched, 1) }

type mockValue struct {
	version int
}

func (v mockValue) Unmarshal(proto.Message) error { return errors.New("unimplemented") }
func (v mockValue) Version() int                  { return v.version }
func (v mockValue) IsNewer(other kv.Value) bool   { return v.version > other.Version() }
