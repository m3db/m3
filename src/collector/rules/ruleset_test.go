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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"

	"github.com/stretchr/testify/require"
)

const (
	testRuleSetKey = "testRuleSet"
)

var (
	testNamespace = []byte("testNamespace")
)

func TestRuleSetProperties(t *testing.T) {
	_, _, rs, _ := testRuleSet()
	rs.namespace = testNamespace
	rs.version = 2
	rs.cutoverNs = 12345
	rs.tombstoned = true

	require.Equal(t, testNamespace, rs.Namespace())
	require.Equal(t, 2, rs.Version())
	require.Equal(t, int64(12345), rs.CutoverNs())
	require.Equal(t, true, rs.Tombstoned())
}

func TestRuleSetMatchNoMatcher(t *testing.T) {
	_, _, rs, _ := testRuleSet()
	require.Equal(t, rules.EmptyMatchResult, rs.Match([]byte("foo"), rs.nowFn()))
}

func TestRuleSetMatchWithMatcher(t *testing.T) {
	_, _, rs, _ := testRuleSet()
	mockMatcher := &mockMatcher{}
	rs.matcher = mockMatcher

	now := rs.nowFn()
	rs.Match([]byte("foo"), now)
	require.Equal(t, []byte("foo"), mockMatcher.id)
	require.Equal(t, now, mockMatcher.t)
}

func TestToRuleSetNilValue(t *testing.T) {
	_, _, rs, _ := testRuleSet()
	_, err := rs.toRuleSet(nil)
	require.Equal(t, errNilValue, err)
}

func TestToRuleSetUnmarshalError(t *testing.T) {
	_, _, rs, _ := testRuleSet()
	_, err := rs.toRuleSet(&mockValue{})
	require.Error(t, err)
}

func TestToRuleSetSuccess(t *testing.T) {
	store, _, rs, _ := testRuleSet()
	proto := &schema.RuleSet{
		Namespace:   string(testNamespace),
		Tombstoned:  false,
		CutoverTime: 123456,
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
	require.Equal(t, int64(123456), actual.CutoverNs())
	require.Equal(t, false, actual.TombStoned())
}

func TestRuleSetProcess(t *testing.T) {
	var (
		inputs = []rules.RuleSet{
			mockRuleSet{namespace: "ns1", version: 1, cutoverNs: 1234, tombstoned: false, matcher: &mockMatcher{}},
			mockRuleSet{namespace: "ns2", version: 2, cutoverNs: 1235, tombstoned: true, matcher: &mockMatcher{}},
			mockRuleSet{namespace: "ns3", version: 3, cutoverNs: 1236, tombstoned: false, matcher: &mockMatcher{}},
			mockRuleSet{namespace: "ns4", version: 4, cutoverNs: 1237, tombstoned: true, matcher: &mockMatcher{}},
			mockRuleSet{namespace: "ns5", version: 5, cutoverNs: 1238, tombstoned: false, matcher: &mockMatcher{}},
		}
	)

	_, cache, rs, _ := testRuleSet()
	memCache := cache.(*memCache)
	for _, input := range inputs {
		rs.process(input)
	}

	require.Equal(t, 5, rs.Version())
	require.Equal(t, int64(1238), rs.CutoverNs())
	require.Equal(t, false, rs.Tombstoned())
	require.NotNil(t, 5, rs.matcher)
	require.Equal(t, 1, len(memCache.namespaces))
	_, exists := memCache.namespaces[string(testNamespace)]
	require.True(t, exists)
}

type mockMatcher struct {
	id []byte
	t  time.Time
}

func (mm *mockMatcher) Match(id []byte, t time.Time) rules.MatchResult {
	mm.id = id
	mm.t = t
	return rules.EmptyMatchResult
}

type mockRuleSet struct {
	namespace  string
	version    int
	cutoverNs  int64
	tombstoned bool
	matcher    *mockMatcher
}

func (r mockRuleSet) Namespace() []byte                   { return []byte(r.namespace) }
func (r mockRuleSet) Version() int                        { return r.version }
func (r mockRuleSet) CutoverNs() int64                    { return r.cutoverNs }
func (r mockRuleSet) TombStoned() bool                    { return r.tombstoned }
func (r mockRuleSet) ActiveSet(t time.Time) rules.Matcher { return r.matcher }

func testRuleSet() (kv.Store, Cache, *ruleSet, Options) {
	store := mem.NewStore()
	cache := newMemCache()
	opts := NewOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetRuleSetKeyFn(func(ns []byte) string { return fmt.Sprintf("/rules/%s", ns) })
	return store, cache, newRuleSet(testNamespace, testNamespacesKey, cache, opts), opts
}
