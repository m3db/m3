// +build integration
//
// Copyright (c) 2016 Uber Technologies, Inc.
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
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

// TestIndexWrites holds index writes for testing.
type TestIndexWrites []testIndexWrite

// MatchesSeriesIters matches index writes with expected series.
func (w TestIndexWrites) MatchesSeriesIters(t *testing.T, seriesIters encoding.SeriesIterators) {
	writesByID := make(map[string]TestIndexWrites)
	for _, wi := range w {
		writesByID[wi.id.String()] = append(writesByID[wi.id.String()], wi)
	}
	require.Equal(t, len(writesByID), seriesIters.Len())
	iters := seriesIters.Iters()
	for _, iter := range iters {
		id := iter.ID().String()
		writes, ok := writesByID[id]
		require.True(t, ok, id)
		writes.matchesSeriesIter(t, iter)
	}
}

func (w TestIndexWrites) matchesSeriesIter(t *testing.T, iter encoding.SeriesIterator) {
	found := make([]bool, len(w))
	count := 0
	for iter.Next() {
		count++
		dp, _, _ := iter.Current()
		for i := 0; i < len(w); i++ {
			if found[i] {
				continue
			}
			wi := w[i]
			if !ident.NewTagIterMatcher(wi.tags.Duplicate()).Matches(iter.Tags().Duplicate()) {
				require.FailNow(t, "tags don't match provided id", iter.ID().String())
			}
			if dp.Timestamp.Equal(wi.ts) && dp.Value == wi.value {
				found[i] = true
				break
			}
		}
	}
	require.Equal(t, len(w), count, iter.ID().String())
	require.NoError(t, iter.Err())
	for i := 0; i < len(found); i++ {
		require.True(t, found[i], iter.ID().String())
	}
}

// Write test data.
func (w TestIndexWrites) Write(t *testing.T, ns ident.ID, s client.Session) {
	for i := 0; i < len(w); i++ {
		wi := w[i]
		require.NoError(t, s.WriteTagged(ns, wi.id, wi.tags.Duplicate(), wi.ts, wi.value, xtime.Second, nil), "%v", wi)
	}
}

// NumIndexed gets number of indexed series.
func (w TestIndexWrites) NumIndexed(t *testing.T, ns ident.ID, s client.Session) int {
	numFound := 0
	for i := 0; i < len(w); i++ {
		wi := w[i]
		q := newQuery(t, wi.tags)
		iter, _, err := s.FetchTaggedIDs(ns, index.Query{Query: q}, index.QueryOptions{
			StartInclusive: wi.ts.Add(-1 * time.Second),
			EndExclusive:   wi.ts.Add(1 * time.Second),
			SeriesLimit:    10})
		if err != nil {
			continue
		}
		if !iter.Next() {
			continue
		}
		cuNs, cuID, cuTag := iter.Current()
		if ns.String() != cuNs.String() {
			continue
		}
		if wi.id.String() != cuID.String() {
			continue
		}
		if !ident.NewTagIterMatcher(wi.tags).Matches(cuTag) {
			continue
		}
		numFound++
	}
	return numFound
}

type testIndexWrite struct {
	id    ident.ID
	tags  ident.TagIterator
	ts    time.Time
	value float64
}

// GenerateTestIndexWrite generates test index writes.
func GenerateTestIndexWrite(periodID, numWrites, numTags int, startTime, endTime time.Time) TestIndexWrites {
	writes := make([]testIndexWrite, 0, numWrites)
	step := endTime.Sub(startTime) / time.Duration(numWrites+1)
	for i := 0; i < numWrites; i++ {
		id, tags := genIDTags(periodID, i, numTags)
		writes = append(writes, testIndexWrite{
			id:    id,
			tags:  tags,
			ts:    startTime.Add(time.Duration(i) * step).Truncate(time.Second),
			value: float64(i),
		})
	}
	return writes
}

type genIDTagsOption func(ident.Tags) ident.Tags

func genIDTags(i int, j int, numTags int, opts ...genIDTagsOption) (ident.ID, ident.TagIterator) {
	id := fmt.Sprintf("foo.%d.%d", i, j)
	tags := make([]ident.Tag, 0, numTags)
	for i := 0; i < numTags; i++ {
		tags = append(tags, ident.StringTag(
			fmt.Sprintf("%s.tagname.%d", id, i),
			fmt.Sprintf("%s.tagvalue.%d", id, i),
		))
	}
	tags = append(tags,
		ident.StringTag("common_i", strconv.Itoa(i)),
		ident.StringTag("common_j", strconv.Itoa(j)),
		ident.StringTag("shared", "shared"))

	result := ident.NewTags(tags...)
	for _, fn := range opts {
		result = fn(result)
	}

	return ident.StringID(id), ident.NewTagsIterator(result)
}

func isIndexed(t *testing.T, s client.Session, ns ident.ID, id ident.ID, tags ident.TagIterator) bool {
	result, err := isIndexedChecked(t, s, ns, id, tags)
	if err != nil {
		return false
	}
	return result
}

func isIndexedChecked(t *testing.T, s client.Session, ns ident.ID, id ident.ID, tags ident.TagIterator) (bool, error) {
	q := newQuery(t, tags)
	iter, _, err := s.FetchTaggedIDs(ns, index.Query{Query: q}, index.QueryOptions{
		StartInclusive: time.Now(),
		EndExclusive:   time.Now(),
		SeriesLimit:    10})
	if err != nil {
		return false, err
	}

	defer iter.Finalize()

	if !iter.Next() {
		return false, nil
	}

	cuNs, cuID, cuTag := iter.Current()
	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("iter err: %v", err)
	}

	if ns.String() != cuNs.String() {
		return false, fmt.Errorf("namespace not matched")
	}
	if id.String() != cuID.String() {
		return false, fmt.Errorf("id not matched")
	}
	if !ident.NewTagIterMatcher(tags).Matches(cuTag) {
		return false, fmt.Errorf("tags did not match")
	}

	return true, nil
}

func newQuery(t *testing.T, tags ident.TagIterator) idx.Query {
	tags = tags.Duplicate()
	filters := make([]idx.Query, 0, tags.Remaining())
	for tags.Next() {
		tag := tags.Current()
		tq := idx.NewTermQuery(tag.Name.Bytes(), tag.Value.Bytes())
		filters = append(filters, tq)
	}
	return idx.NewConjunctionQuery(filters...)
}
