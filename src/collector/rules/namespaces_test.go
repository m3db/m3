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

package rules

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/id"

	"github.com/stretchr/testify/require"
)

const (
	testNamespacesKey = "testNamespaces"
)

func TestNamespacesWatchAndClose(t *testing.T) {
	store, _, nss, _ := testNamespaces()
	proto := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
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
	require.Equal(t, 1, len(nss.rules))
	nss.Close()
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
	proto := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
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
		rs := newRuleSet(input.namespace, input.id, cache, opts)
		rs.Value = &mockRuntimeValue{key: input.id}
		rs.version = input.version
		rs.tombstoned = input.tombstoned
		nss.rules[xid.HashFn(input.namespace)] = rs
		c.namespaces[string(input.namespace)] = memResults{source: rs}
	}

	update := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&schema.Namespace{
				Name: "barNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
			&schema.Namespace{
				Name: "bazNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&schema.Namespace{
				Name: "catNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 3,
						Tombstoned:        true,
					},
				},
			},
			&schema.Namespace{
				Name:      "mehNs",
				Snapshots: nil,
			},
		},
	}

	nssValue, err := rules.NewNamespaces(5, update)
	require.NoError(t, err)
	require.NoError(t, nss.process(nssValue))
	require.Equal(t, 5, len(nss.rules))
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
		rs, exists := nss.rules[xid.HashFn([]byte(ns.Name))]
		require.True(t, exists)
		require.Equal(t, expected[i].key, rs.Key())
		require.Equal(t, rs, c.namespaces[string(ns.Name)].source.(*ruleSet))
		mv, ok := rs.Value.(*mockRuntimeValue)
		if !ok {
			continue
		}
		require.Equal(t, expected[i].watched, mv.numWatched)
		require.Equal(t, expected[i].unwatched, mv.numUnwatched)
	}
}

func testNamespaces() (kv.Store, Cache, *namespaces, Options) {
	store := mem.NewStore()
	cache := newMemCache()
	opts := NewOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetNamespacesKey(testNamespacesKey)
	return store, cache, newNamespaces(testNamespacesKey, cache, opts), opts
}

type memResults struct {
	results map[string]rules.MatchResult
	source  Source
}

type memCache struct {
	sync.RWMutex

	namespaces map[string]memResults
}

func newMemCache() Cache {
	return &memCache{namespaces: make(map[string]memResults)}
}

func (c *memCache) Match(namespace []byte, id []byte) rules.MatchResult {
	c.RLock()
	defer c.RUnlock()
	if results, exists := c.namespaces[string(namespace)]; exists {
		return results.results[string(id)]
	}
	return rules.EmptyMatchResult
}

func (c *memCache) Register(namespace []byte, source Source) {
	c.Lock()
	defer c.Unlock()

	results, exists := c.namespaces[string(namespace)]
	if !exists {
		results = memResults{
			results: make(map[string]rules.MatchResult),
			source:  source,
		}
	} else {
		results.source = source
	}
	c.namespaces[string(namespace)] = results
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
	watchErr     error
	numUnwatched int32
}

func (mv *mockRuntimeValue) Key() string  { return mv.key }
func (mv *mockRuntimeValue) Watch() error { atomic.AddInt32(&mv.numWatched, 1); return nil }
func (mv *mockRuntimeValue) Unwatch()     { atomic.AddInt32(&mv.numUnwatched, 1) }

type mockValue struct {
	version int
	cutover time.Time
}

func (v mockValue) Unmarshal(proto.Message) error { return errors.New("unimplemented") }
func (v mockValue) Version() int                  { return v.version }
func (v mockValue) IsNewer(other kv.Value) bool   { return v.version > other.Version() }
