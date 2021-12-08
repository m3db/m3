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

package m3

import (
	"testing"

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestNewRollupID(t *testing.T) {
	var (
		name     = []byte("foo")
		tagPairs = []id.TagPair{
			{Name: []byte("tagName1"), Value: []byte("tagValue1")},
			{Name: []byte("tagName2"), Value: []byte("tagValue2")},
			{Name: []byte("tagName0"), Value: []byte("tagValue0")},
		}
	)
	expected := []byte("m3+foo+m3_rollup=true,tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2")
	id, err := NewRollupIDer().ID(name, tagPairs)
	require.NoError(t, err)
	require.Equal(t, expected, id)
}

func TestIsRollupIDNilIterator(t *testing.T) {
	inputs := []struct {
		name     []byte
		tags     []byte
		expected bool
	}{
		{name: []byte("foo"), tags: []byte("a1=b1,m3_rollup=true,a2=b2"), expected: true},
		{name: []byte("foo.bar.baz"), expected: false},
		{name: []byte("foo"), tags: []byte("a1=b1,a2=b2"), expected: false},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, IsRollupID(input.name, input.tags, nil), string(input.tags))
	}
}

func TestIsRollupIDExternalIterator(t *testing.T) {
	inputs := []struct {
		name     []byte
		tags     []byte
		expected bool
	}{
		{name: []byte("foo"), tags: []byte("a1=b1,m3_rollup=true,a2=b2"), expected: true},
		{name: []byte("foo.bar.baz"), expected: false},
		{name: []byte("foo"), tags: []byte("a1=b1,a2=b2"), expected: false},
	}
	p := id.NewSortedTagIteratorPool(pool.NewObjectPoolOptions())
	p.Init(func() id.SortedTagIterator {
		return NewPooledSortedTagIterator(nil, p)
	})
	for _, input := range inputs {
		require.Equal(t, input.expected, IsRollupID(input.name, input.tags, p))
	}
}

func TestMetricIDTagValue(t *testing.T) {
	iterPool := id.NewSortedTagIteratorPool(nil)
	iterPool.Init(func() id.SortedTagIterator {
		return NewPooledSortedTagIterator(nil, iterPool)
	})
	inputs := []struct {
		id            id.ID
		tagName       []byte
		expectedValue []byte
		expectedFound bool
	}{
		{
			id:            NewID([]byte("m3+foo+tagName1=tagValue1,tagName2=tagValue2"), iterPool),
			tagName:       []byte("tagName1"),
			expectedValue: []byte("tagValue1"),
			expectedFound: true,
		},
		{
			id:            NewID([]byte("m3+foo+tagName1=tagValue1,tagName2=tagValue2"), iterPool),
			tagName:       []byte("tagName2"),
			expectedValue: []byte("tagValue2"),
			expectedFound: true,
		},
		{
			id:            NewID([]byte("m3+foo+tagName1=tagValue1,tagName2=tagValue2"), iterPool),
			tagName:       []byte("tagName3"),
			expectedValue: nil,
			expectedFound: false,
		},
		{
			id:            NewID([]byte("illformed+tagName1=tagValue1,tagName2=tagValue2"), iterPool),
			tagName:       []byte("tagName1"),
			expectedValue: nil,
			expectedFound: false,
		},
	}
	for _, input := range inputs {
		value, found := input.id.TagValue(input.tagName)
		require.Equal(t, input.expectedValue, value)
		require.Equal(t, input.expectedFound, found)
	}
}

func TestNameAndTags(t *testing.T) {
	inputs := []struct {
		id           []byte
		expectedName []byte
		expectedTags []byte
		expectedErr  error
	}{
		{
			id:           []byte("stats.m3+foo+tagName1=tagValue1"),
			expectedName: []byte("foo"),
			expectedTags: []byte("tagName1=tagValue1"),
			expectedErr:  nil,
		},
		{
			id:           []byte("m3+foo+tagName1=tagValue1"),
			expectedName: []byte("foo"),
			expectedTags: []byte("tagName1=tagValue1"),
			expectedErr:  nil,
		},
		{
			id:           []byte("m3+foo+tagName1=tagValue1,tagName2=tagValue2"),
			expectedName: []byte("foo"),
			expectedTags: []byte("tagName1=tagValue1,tagName2=tagValue2"),
			expectedErr:  nil,
		},
		{
			id:           []byte("illformed"),
			expectedName: nil,
			expectedTags: nil,
			expectedErr:  errInvalidM3Metric,
		},
		{
			id:           []byte("m34+illformed+tagName1=tagValue1"),
			expectedName: nil,
			expectedTags: nil,
			expectedErr:  errInvalidM3Metric,
		},
		{
			id:           []byte("m3+illformed"),
			expectedName: nil,
			expectedTags: nil,
			expectedErr:  errInvalidM3Metric,
		},
		{
			id:           []byte("m3+illformed+tagName1,"),
			expectedName: []byte("illformed"),
			expectedTags: []byte("tagName1,"),
			expectedErr:  nil,
		},
	}
	for _, input := range inputs {
		name, tags, err := NameAndTags(input.id)
		require.Equal(t, input.expectedName, name)
		require.Equal(t, input.expectedTags, tags)
		require.Equal(t, input.expectedErr, err)
	}
}

func TestSortedTagIterator(t *testing.T) {
	inputs := []struct {
		sortedTagPairs []byte
		expectedPairs  []id.TagPair
		expectedErr    error
	}{
		{
			sortedTagPairs: []byte("tagName1=tagValue1"),
			expectedPairs: []id.TagPair{
				{Name: []byte("tagName1"), Value: []byte("tagValue1")},
			},
			expectedErr: nil,
		},
		{
			sortedTagPairs: []byte("tagName1=tagValue1,tagName2=tagValue2,tagName3=tagValue3"),
			expectedPairs: []id.TagPair{
				{Name: []byte("tagName1"), Value: []byte("tagValue1")},
				{Name: []byte("tagName2"), Value: []byte("tagValue2")},
				{Name: []byte("tagName3"), Value: []byte("tagValue3")},
			},
			expectedErr: nil,
		},
		{
			sortedTagPairs: []byte("tagName1"),
			expectedPairs:  nil,
			expectedErr:    errInvalidM3Metric,
		},
	}

	for _, input := range inputs {
		it := NewSortedTagIterator(input.sortedTagPairs)
		var result []id.TagPair
		for it.Next() {
			name, value := it.Current()
			result = append(result, id.TagPair{Name: name, Value: value})
		}
		require.Equal(t, input.expectedErr, it.Err())
		require.Equal(t, input.expectedPairs, result)
	}
}
