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
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestMatcherCreateWatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kvStore := kv.NewMockStore(ctrl)
	kvStore.EXPECT().Watch(testNamespacesKey).Return(nil, runtime.CreateWatchError{})
	opts := NewOptions().
		SetInitWatchTimeout(10 * time.Millisecond).
		SetNamespacesKey(testNamespacesKey).
		SetKVStore(kvStore)

	_, err := NewMatcher(newMemCache(), opts)
	require.Error(t, err)
	_, ok := err.(runtime.CreateWatchError)
	require.True(t, ok)
}

func TestMatcherInitializeValueError(t *testing.T) {
	memStore := mem.NewStore()
	opts := NewOptions().
		SetInitWatchTimeout(10 * time.Millisecond).
		SetNamespacesKey(testNamespacesKey).
		SetKVStore(memStore)

	matcher, err := NewMatcher(newMemCache(), opts)
	require.NoError(t, err)
	require.NotNil(t, matcher)
}

func TestMatcherMatchDoesNotExist(t *testing.T) {
	id := &testMetricID{
		id:         []byte("foo"),
		tagValueFn: func(tagName []byte) ([]byte, bool) { return nil, false },
	}
	_, matcher := testMatcher(t)
	require.Equal(t, rules.EmptyMatchResult, matcher.Match(id))
}

func TestMatcherMatchExists(t *testing.T) {
	var (
		ns = "ns/foo"
		id = &testMetricID{
			id:         []byte("foo"),
			tagValueFn: func(tagName []byte) ([]byte, bool) { return []byte(ns), true },
		}
		res    = rules.NewMatchResult(1, 0, math.MaxInt64, nil, nil)
		memRes = memResults{results: map[string]rules.MatchResult{"foo": res}}
	)
	cache, matcher := testMatcher(t)
	c := cache.(*memCache)
	c.namespaces[ns] = memRes
	require.Equal(t, res, matcher.Match(id))
}

func TestMatcherClose(t *testing.T) {
	_, matcher := testMatcher(t)
	require.NoError(t, matcher.Close())
}

func testMatcher(t *testing.T) (Cache, Matcher) {
	var (
		store = mem.NewStore()
		cache = newMemCache()
		opts  = NewOptions().
			SetClockOptions(clock.NewOptions()).
			SetInstrumentOptions(instrument.NewOptions()).
			SetInitWatchTimeout(100 * time.Millisecond).
			SetKVStore(store).
			SetNamespacesKey(testNamespacesKey).
			SetNamespaceTag([]byte("namespace")).
			SetDefaultNamespace([]byte("default")).
			SetRuleSetKeyFn(defaultRuleSetKeyFn).
			SetRuleSetOptions(rules.NewOptions())
		proto = &schema.Namespaces{
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
	)
	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)

	m, err := NewMatcher(cache, opts)
	require.NoError(t, err)
	return cache, m
}

type tagValueFn func(tagName []byte) ([]byte, bool)

type testMetricID struct {
	id         []byte
	tagValueFn tagValueFn
}

func (id *testMetricID) Bytes() []byte                          { return id.id }
func (id *testMetricID) TagValue(tagName []byte) ([]byte, bool) { return id.tagValueFn(tagName) }
