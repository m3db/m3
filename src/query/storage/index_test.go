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

package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3x/ident"
)

var (
	testID   = ident.StringID("test_id")
	testNS   = ident.StringID("test_namespace")
	testTags = models.Tags{"t1": "v1", "t2": "v2"}
	now      = time.Now()
)

func TestTagsToIdentTagIterator(t *testing.T) {
	tagIter := TagsToIdentTagIterator(testTags)
	defer tagIter.Close()

	tags := make(models.Tags, len(testTags))
	for tagIter.Next() {
		tags[tagIter.Current().Name.String()] = tagIter.Current().Value.String()
	}
	assert.Equal(t, testTags, tags)
}

func TestFromM3IdentToMetric(t *testing.T) {
	tagIters := TagsToIdentTagIterator(testTags)
	metric, err := FromM3IdentToMetric(testNS, testID, tagIters)
	require.NoError(t, err)

	assert.Equal(t, testID.String(), metric.ID)
	assert.Equal(t, testNS.String(), metric.Namespace)
	assert.Equal(t, testTags, metric.Tags)
}

func TestFromIdentTagIteratorToTags(t *testing.T) {
	tagIters := TagsToIdentTagIterator(testTags)
	tags, err := FromIdentTagIteratorToTags(tagIters)
	require.NoError(t, err)

	assert.Equal(t, testTags, tags)
}

func TestFetchQueryToM3Query(t *testing.T) {
	matchers := models.Matchers{
		{
			Type:  models.MatchEqual,
			Name:  "t1",
			Value: "v1",
		},
	}

	fetchQuery := &FetchQuery{
		Raw:         "up",
		TagMatchers: matchers,
		Start:       now.Add(-5 * time.Minute),
		End:         now,
		Interval:    15 * time.Second,
	}

	m3Query, err := FetchQueryToM3Query(fetchQuery)
	require.NoError(t, err)
	assert.Equal(t, "conjunction(term(t1, v1))", m3Query.String())
}
