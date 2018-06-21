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
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3db/src/m3ninx/idx"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type testIndexWrites []testIndexWrite

func (w testIndexWrites) matchesSeriesIters(t *testing.T, seriesIters encoding.SeriesIterators) {
	writesByID := make(map[string]testIndexWrites)
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

func (w testIndexWrites) matchesSeriesIter(t *testing.T, iter encoding.SeriesIterator) {
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

func (w testIndexWrites) write(t *testing.T, ns ident.ID, s client.Session) {
	for i := 0; i < len(w); i++ {
		wi := w[i]
		require.NoError(t, s.WriteTagged(ns, wi.id, wi.tags.Duplicate(), wi.ts, wi.value, xtime.Second, nil), "%v", wi)
	}
}

func (w testIndexWrites) numIndexed(t *testing.T, ns ident.ID, s client.Session) int {
	numFound := 0
	for i := 0; i < len(w); i++ {
		wi := w[i]
		q := newQuery(t, wi.tags)
		iter, _, err := s.FetchTaggedIDs(ns, index.Query{q}, index.QueryOptions{
			StartInclusive: wi.ts.Add(-1 * time.Second),
			EndExclusive:   wi.ts.Add(1 * time.Second),
			Limit:          10})
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

func generateTestIndexWrite(periodID, numWrites, numTags int, startTime, endTime time.Time) testIndexWrites {
	writes := make([]testIndexWrite, 0, numWrites)
	step := endTime.Sub(startTime) / time.Duration(numWrites+1)
	for i := 1; i <= numWrites; i++ {
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

func genIDTags(i int, j int, numTags int) (ident.ID, ident.TagIterator) {
	id := fmt.Sprintf("foo.%d.%d", i, j)
	tags := make([]ident.Tag, 0, numTags)
	for i := 0; i < numTags; i++ {
		tags = append(tags, ident.StringTag(
			fmt.Sprintf("%s.tagname.%d", id, i),
			fmt.Sprintf("%s.tagvalue.%d", id, i),
		))
	}
	tags = append(tags,
		ident.StringTag("commoni", fmt.Sprintf("%d", i)),
		ident.StringTag("shared", "shared"))
	return ident.StringID(id), ident.NewTagsIterator(ident.NewTags(tags...))
}

func isIndexed(t *testing.T, s client.Session, ns ident.ID, id ident.ID, tags ident.TagIterator) bool {
	q := newQuery(t, tags)
	iter, _, err := s.FetchTaggedIDs(ns, index.Query{q}, index.QueryOptions{
		StartInclusive: time.Now(),
		EndExclusive:   time.Now(),
		Limit:          10})
	if err != nil {
		return false
	}
	if !iter.Next() {
		return false
	}
	cuNs, cuID, cuTag := iter.Current()
	if ns.String() != cuNs.String() {
		return false
	}
	if id.String() != cuID.String() {
		return false
	}
	return ident.NewTagIterMatcher(tags).Matches(cuTag)
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
