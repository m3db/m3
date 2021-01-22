// Copyright (c) 2021 Uber Technologies, Inc.
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

package convert_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustToRpcTime(t *testing.T, ts time.Time) int64 {
	r, err := convert.ToValue(ts, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	return r
}

func allQueryTestCase(t *testing.T) (idx.Query, []byte) {
	q := idx.NewAllQuery()
	d, err := idx.Marshal(q)
	require.NoError(t, err)
	return q, d
}

func fieldQueryTestCase(t *testing.T) (idx.Query, []byte) {
	q1 := idx.NewFieldQuery([]byte("dat"))
	data, err := idx.Marshal(q1)
	require.NoError(t, err)
	return q1, data
}

func termQueryTestCase(t *testing.T) (idx.Query, []byte) {
	q1 := idx.NewTermQuery([]byte("dat"), []byte("baz"))
	data, err := idx.Marshal(q1)
	require.NoError(t, err)
	return q1, data
}

func regexpQueryTestCase(t *testing.T) (idx.Query, []byte) {
	q2, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	data, err := idx.Marshal(q2)
	require.NoError(t, err)
	return q2, data
}

func negateTermQueryTestCase(t *testing.T) (idx.Query, []byte) {
	q3 := idx.NewNegationQuery(idx.NewTermQuery([]byte("foo"), []byte("bar")))
	data, err := idx.Marshal(q3)
	require.NoError(t, err)
	return q3, data
}

func negateRegexpQueryTestCase(t *testing.T) (idx.Query, []byte) {
	inner, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	q4 := idx.NewNegationQuery(inner)
	data, err := idx.Marshal(q4)
	require.NoError(t, err)
	return q4, data
}

func conjunctionQueryATestCase(t *testing.T) (idx.Query, []byte) {
	q1, _ := termQueryTestCase(t)
	q2, _ := regexpQueryTestCase(t)
	q3, _ := negateTermQueryTestCase(t)
	q4, _ := negateRegexpQueryTestCase(t)
	q := idx.NewConjunctionQuery(q1, q2, q3, q4)
	data, err := idx.Marshal(q)
	require.NoError(t, err)
	return q, data
}

func TestConvertFetchTaggedRequest(t *testing.T) {
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	ns := ident.StringID("abc")
	opts := index.QueryOptions{
		StartInclusive: time.Now().Add(-900 * time.Hour),
		EndExclusive:   time.Now(),
		SeriesLimit:    int(seriesLimit),
		DocsLimit:      int(docsLimit),
	}
	fetchData := true
	requestSkeleton := &rpc.FetchTaggedRequest{
		NameSpace:   ns.Bytes(),
		RangeStart:  mustToRpcTime(t, opts.StartInclusive),
		RangeEnd:    mustToRpcTime(t, opts.EndExclusive),
		FetchData:   fetchData,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	}
	requireEqual := func(a, b interface{}) {
		d := cmp.Diff(a, b)
		assert.Equal(t, "", d, d)
	}

	type inputFn func(t *testing.T) (idx.Query, []byte)

	for _, pools := range []struct {
		name string
		pool convert.FetchTaggedConversionPools
	}{
		{"nil pools", nil},
		{"valid pools", newTestPools()},
	} {
		testCases := []struct {
			name string
			fn   inputFn
		}{
			{"Field Query", fieldQueryTestCase},
			{"Term Query", termQueryTestCase},
			{"Regexp Query", regexpQueryTestCase},
			{"Negate Term Query", negateTermQueryTestCase},
			{"Negate Regexp Query", negateRegexpQueryTestCase},
			{"Conjunction Query A", conjunctionQueryATestCase},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("(%s pools) Forward %s", pools.name, tc.name), func(t *testing.T) {
				q, rpcQ := tc.fn(t)
				expectedReq := &(*requestSkeleton)
				expectedReq.Query = rpcQ
				observedReq, err := convert.ToRPCFetchTaggedRequest(ns, index.Query{Query: q}, opts, fetchData)
				require.NoError(t, err)
				requireEqual(expectedReq, &observedReq)
			})
			t.Run(fmt.Sprintf("(%s pools) Backward %s", pools.name, tc.name), func(t *testing.T) {
				expectedQuery, rpcQ := tc.fn(t)
				rpcRequest := &(*requestSkeleton)
				rpcRequest.Query = rpcQ
				id, observedQuery, observedOpts, fetch, err := convert.FromRPCFetchTaggedRequest(rpcRequest, pools.pool)
				require.NoError(t, err)
				require.Equal(t, ns.String(), id.String())
				require.True(t, index.NewQueryMatcher(index.Query{Query: expectedQuery}).Matches(observedQuery))
				requireEqual(fetchData, fetch)
				requireEqual(opts, observedOpts)
			})
		}
	}
}

func TestConvertAggregateRawQueryRequest(t *testing.T) {
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
		ns                = ident.StringID("abc")
	)
	opts := index.AggregationOptions{
		QueryOptions: index.QueryOptions{
			StartInclusive: time.Now().Add(-900 * time.Hour),
			EndExclusive:   time.Now(),
			SeriesLimit:    int(seriesLimit),
			DocsLimit:      int(docsLimit),
		},
		Type: index.AggregateTagNamesAndValues,
		FieldFilter: index.AggregateFieldFilter{
			[]byte("some"),
			[]byte("string"),
		},
	}
	requestSkeleton := &rpc.AggregateQueryRawRequest{
		NameSpace:   ns.Bytes(),
		RangeStart:  mustToRpcTime(t, opts.StartInclusive),
		RangeEnd:    mustToRpcTime(t, opts.EndExclusive),
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
		TagNameFilter: [][]byte{
			[]byte("some"),
			[]byte("string"),
		},
		AggregateQueryType: rpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE,
	}
	requireEqual := func(a, b interface{}) {
		d := cmp.Diff(a, b)
		assert.Equal(t, "", d, d)
	}

	type inputFn func(t *testing.T) (idx.Query, []byte)

	for _, pools := range []struct {
		name string
		pool convert.FetchTaggedConversionPools
	}{
		{"nil pools", nil},
		{"valid pools", newTestPools()},
	} {
		testCases := []struct {
			name string
			fn   inputFn
		}{
			{"All Query", allQueryTestCase},
			{"Field Query", fieldQueryTestCase},
			{"Term Query", termQueryTestCase},
			{"Regexp Query", regexpQueryTestCase},
			{"Negate Term Query", negateTermQueryTestCase},
			{"Negate Regexp Query", negateRegexpQueryTestCase},
			{"Conjunction Query A", conjunctionQueryATestCase},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s forward %s", pools.name, tc.name), func(t *testing.T) {
				q, rpcQ := tc.fn(t)
				expectedReq := &(*requestSkeleton)
				expectedReq.Query = rpcQ
				observedReq, err := convert.ToRPCAggregateQueryRawRequest(ns, index.Query{Query: q}, opts)
				require.NoError(t, err)
				requireEqual(expectedReq, &observedReq)
			})
			t.Run(fmt.Sprintf("%s backward %s", pools.name, tc.name), func(t *testing.T) {
				expectedQuery, rpcQ := tc.fn(t)
				rpcRequest := &(*requestSkeleton)
				rpcRequest.Query = rpcQ
				id, observedQuery, observedOpts, err := convert.FromRPCAggregateQueryRawRequest(rpcRequest, pools.pool)
				require.NoError(t, err)
				require.Equal(t, ns.String(), id.String())
				require.True(t, index.NewQueryMatcher(index.Query{Query: expectedQuery}).Matches(observedQuery))
				requireEqual(opts, observedOpts)
			})
		}
	}
}

type testPools struct {
	id      ident.Pool
	wrapper xpool.CheckedBytesWrapperPool
}

func newTestPools() *testPools {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	id := ident.NewPool(nil, ident.PoolOptions{
		IDPoolOptions:           poolOpts,
		TagsPoolOptions:         poolOpts,
		TagsIteratorPoolOptions: poolOpts,
	})
	wrapper := xpool.NewCheckedBytesWrapperPool(poolOpts)
	wrapper.Init()
	return &testPools{id, wrapper}
}

var _ convert.FetchTaggedConversionPools = &testPools{}

func (t *testPools) ID() ident.Pool                                     { return t.id }
func (t *testPools) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool { return t.wrapper }
