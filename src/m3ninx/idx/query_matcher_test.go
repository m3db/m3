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

package idx_test

import (
	"testing"

	"github.com/m3db/m3ninx/idx"

	"github.com/stretchr/testify/require"
)

func TestQueryMatcherTermQuery(t *testing.T) {
	for _, tc := range []struct {
		left     idx.Query
		right    idx.Query
		expected bool
	}{
		{
			left:     idx.NewTermQuery([]byte("abc"), []byte("def")),
			right:    idx.NewTermQuery([]byte("abc"), []byte("def")),
			expected: true,
		},
		{
			left:     idx.NewTermQuery([]byte("abc"), []byte("def")),
			right:    idx.NewTermQuery([]byte("abc1"), []byte("def")),
			expected: false,
		},
		{
			left:     idx.NewTermQuery([]byte("abc"), []byte("def")),
			right:    idx.NewTermQuery([]byte("abc"), []byte("def1")),
			expected: false,
		},
	} {
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.left).Matches(tc.right))
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.right).Matches(tc.left))
	}
}

func TestQueryMatcherRegexpQuery(t *testing.T) {
	for _, tc := range []struct {
		left     idx.Query
		right    idx.Query
		expected bool
	}{
		{
			left:     mustCreateRegexpQuery(t, []byte("abc"), []byte("def")),
			right:    mustCreateRegexpQuery(t, []byte("abc"), []byte("def")),
			expected: true,
		},
		{
			left:     mustCreateRegexpQuery(t, []byte("abc"), []byte("def")),
			right:    mustCreateRegexpQuery(t, []byte("abc1"), []byte("def")),
			expected: false,
		},
		{
			left:     mustCreateRegexpQuery(t, []byte("abc"), []byte("def")),
			right:    mustCreateRegexpQuery(t, []byte("abc"), []byte("def1")),
			expected: false,
		},
	} {
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.left).Matches(tc.right))
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.right).Matches(tc.left))
	}
}

func TestQueryMatcherNegationQuery(t *testing.T) {
	for _, tc := range []struct {
		left     idx.Query
		right    idx.Query
		expected bool
	}{
		{
			left:     idx.NewNegationQuery(idx.NewTermQuery([]byte("abc"), []byte("def"))),
			right:    idx.NewNegationQuery(idx.NewTermQuery([]byte("abc"), []byte("def"))),
			expected: true,
		},
		{
			left:     idx.NewNegationQuery(idx.NewTermQuery([]byte("abc"), []byte("def"))),
			right:    idx.NewNegationQuery(idx.NewTermQuery([]byte("abc"), []byte("efg"))),
			expected: false,
		},
	} {
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.left).Matches(tc.right))
		require.Equal(t, tc.expected, idx.NewQueryMatcher(tc.right).Matches(tc.left))
	}
}

func TestQueryMatcherTermRegexpMismatch(t *testing.T) {
	q0 := idx.NewTermQuery([]byte("abc"), []byte("def"))
	q1, err := idx.NewRegexpQuery([]byte("abc"), []byte("def"))
	require.NoError(t, err)
	require.False(t, idx.NewQueryMatcher(q0).Matches(q1))
}

func TestQueryMatcherConjunctionQuery(t *testing.T) {
	tq0 := idx.NewTermQuery([]byte("abc0"), []byte("def"))
	tq1 := idx.NewTermQuery([]byte("abc1"), []byte("def"))
	rq, err := idx.NewRegexpQuery([]byte("abc2"), []byte("def"))
	require.NoError(t, err)
	q := idx.NewConjunctionQuery(tq0, tq1, rq)
	require.True(t, idx.NewQueryMatcher(q).Matches(q))
}

func TestQueryMatcherTermConjMismatch(t *testing.T) {
	q0 := idx.NewTermQuery([]byte("abc"), []byte("def"))
	tq1 := idx.NewTermQuery([]byte("abc1"), []byte("def"))
	rq, err := idx.NewRegexpQuery([]byte("abc2"), []byte("def"))
	require.NoError(t, err)
	q1 := idx.NewConjunctionQuery(q0, tq1, rq)
	require.False(t, idx.NewQueryMatcher(q0).Matches(q1))
}

func TestQueryMatcherDisjunctionQuery(t *testing.T) {
	tq0 := idx.NewTermQuery([]byte("abc0"), []byte("def"))
	tq1 := idx.NewTermQuery([]byte("abc1"), []byte("def"))
	rq, err := idx.NewRegexpQuery([]byte("abc2"), []byte("def"))
	require.NoError(t, err)
	q := idx.NewDisjunctionQuery(tq0, tq1, rq)
	require.True(t, idx.NewQueryMatcher(q).Matches(q))
}

func mustCreateRegexpQuery(t *testing.T, field, regexp []byte) idx.Query {
	q, err := idx.NewRegexpQuery(field, regexp)
	require.NoError(t, err)
	return q
}
