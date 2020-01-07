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

package integration

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type verifyQueryMetadataResultsOptions struct {
	namespace  ident.ID
	exhaustive bool
	expected   []generate.Series
}

type verifyQueryMetadataResult struct {
	series  generate.Series
	matched bool
}

func verifyQueryMetadataResults(
	t *testing.T,
	iter client.TaggedIDsIterator,
	exhaustive bool,
	opts verifyQueryMetadataResultsOptions,
) {
	assert.Equal(t, opts.exhaustive, exhaustive)

	expected := make(map[string]*verifyQueryMetadataResult, len(opts.expected))
	for _, series := range opts.expected {
		expected[series.ID.String()] = &verifyQueryMetadataResult{
			series:  series,
			matched: false,
		}
	}

	compared := 0
	for iter.Next() {
		compared++

		ns, id, tags := iter.Current()
		assert.True(t, opts.namespace.Equal(ns))

		idStr := id.String()
		result, ok := expected[idStr]
		require.True(t, ok,
			fmt.Sprintf("not expecting ID: %s", idStr))

		expectedTagsIter := ident.NewTagsIterator(result.series.Tags)
		matcher := ident.NewTagIterMatcher(expectedTagsIter)
		assert.True(t, matcher.Matches(tags),
			fmt.Sprintf("tags not matching for ID: %s", idStr))

		result.matched = true
	}
	require.NoError(t, iter.Err())

	var matched, notMatched []string
	for _, elem := range expected {
		if elem.matched {
			matched = append(matched, elem.series.ID.String())
			continue
		}
		notMatched = append(notMatched, elem.series.ID.String())
	}

	assert.Equal(t, len(expected), compared,
		fmt.Sprintf("matched: %v, not matched: %v", matched, notMatched))
}

type tagValue string
type tagName string
type aggregateTagValues map[tagValue]struct{}
type aggregateTags map[tagName]aggregateTagValues
type tagValueSeen bool

type verifyQueryAggregateMetadataResultsOptions struct {
	exhaustive bool
	expected   aggregateTags
}

func verifyQueryAggregateMetadataResults(
	t *testing.T,
	iter client.AggregatedTagsIterator,
	exhaustive bool,
	opts verifyQueryAggregateMetadataResultsOptions,
) {
	assert.Equal(t, opts.exhaustive, exhaustive)

	expected := make(map[tagName]map[tagValue]tagValueSeen, len(opts.expected))
	for name, values := range opts.expected {
		expected[name] = map[tagValue]tagValueSeen{}
		for value := range values {
			expected[name][value] = tagValueSeen(false)
		}
	}

	for iter.Next() {
		name, values := iter.Current()

		result, ok := expected[tagName(name.String())]
		require.True(t, ok,
			fmt.Sprintf("not expecting tag: %s", name.String()))

		for values.Next() {
			value := values.Current()

			entry, ok := result[tagValue(value.String())]
			require.True(t, ok,
				fmt.Sprintf("not expecting tag value: name=%s, value=%s",
					name.String(), value.String()))
			require.False(t, bool(entry))

			result[tagValue(value.String())] = tagValueSeen(true)
		}

		require.NoError(t, values.Err())
	}
	require.NoError(t, iter.Err())

	var matched, notMatched []string
	for name, values := range expected {
		for value, valueMatched := range values {
			elem := fmt.Sprintf("(tagName=%s, tagValue=%s)", name, value)
			if valueMatched {
				matched = append(matched, elem)
				continue
			}
			notMatched = append(notMatched, elem)
		}
	}

	assert.Equal(t, 0, len(notMatched),
		fmt.Sprintf("matched: %v, not matched: %v", matched, notMatched))
}
