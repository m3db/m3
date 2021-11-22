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
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// TestIndexWrites holds index writes for testing.
type TestIndexWrites []TestIndexWrite

// TestSeriesIterator is a minimal subset of encoding.SeriesIterator.
type TestSeriesIterator interface {
	encoding.Iterator

	// ID gets the ID of the series.
	ID() ident.ID

	// Tags returns an iterator over the tags associated with the ID.
	Tags() ident.TagIterator
}

// TestSeriesIterators is a an iterator over TestSeriesIterator.
type TestSeriesIterators interface {

	// Next moves to the next item.
	Next() bool

	// Current returns the current value.
	Current() TestSeriesIterator
}

type testSeriesIterators struct {
	encoding.SeriesIterators
	idx int
}

func (t *testSeriesIterators) Next() bool {
	if t.idx >= t.Len() {
		return false
	}
	t.idx++

	return true
}

func (t *testSeriesIterators) Current() TestSeriesIterator {
	return t.Iters()[t.idx-1]
}

// MatchesSeriesIters matches index writes with expected series.
func (w TestIndexWrites) MatchesSeriesIters(
	t *testing.T,
	seriesIters encoding.SeriesIterators,
) {
	actualCount := w.MatchesTestSeriesIters(t, &testSeriesIterators{SeriesIterators: seriesIters})

	uniqueIDs := make(map[string]struct{})
	for _, wi := range w {
		uniqueIDs[wi.ID.String()] = struct{}{}
	}
	require.Equal(t, len(uniqueIDs), actualCount)
}

// MatchesTestSeriesIters matches index writes with expected test series.
func (w TestIndexWrites) MatchesTestSeriesIters(
	t *testing.T,
	seriesIters TestSeriesIterators,
) int {
	writesByID := make(map[string]TestIndexWrites)
	for _, wi := range w {
		writesByID[wi.ID.String()] = append(writesByID[wi.ID.String()], wi)
	}
	var actualCount int
	for seriesIters.Next() {
		iter := seriesIters.Current()
		id := iter.ID().String()
		writes, ok := writesByID[id]
		require.True(t, ok, id)
		writes.matchesSeriesIter(t, iter)
		actualCount++
	}

	return actualCount
}

func (w TestIndexWrites) matchesSeriesIter(t *testing.T, iter TestSeriesIterator) {
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
			if !ident.NewTagIterMatcher(wi.Tags.Duplicate()).Matches(iter.Tags().Duplicate()) {
				require.FailNow(t, "tags don't match provided id", iter.ID().String())
			}
			if dp.TimestampNanos.Equal(wi.Timestamp) && dp.Value == wi.Value {
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

// Write writes test data and asserts the result.
func (w TestIndexWrites) Write(t *testing.T, ns ident.ID, s client.Session) {
	require.NoError(t, w.WriteAttempt(ns, s))
}

// WriteAttempt writes test data and returns an error if encountered.
func (w TestIndexWrites) WriteAttempt(ns ident.ID, s client.Session) error {
	for i := 0; i < len(w); i++ {
		wi := w[i]
		err := s.WriteTagged(ns, wi.ID, wi.Tags.Duplicate(), wi.Timestamp,
			wi.Value, xtime.Second, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// NumIndexed gets number of indexed series.
func (w TestIndexWrites) NumIndexed(t *testing.T, ns ident.ID, s client.Session) int {
	return w.NumIndexedWithOptions(t, ns, s, NumIndexedOptions{})
}

// NumIndexedOptions is options when performing num indexed check.
type NumIndexedOptions struct {
	Logger *zap.Logger
}

// NumIndexedWithOptions gets number of indexed series with a set of options.
func (w TestIndexWrites) NumIndexedWithOptions(
	t *testing.T,
	ns ident.ID,
	s client.Session,
	opts NumIndexedOptions,
) int {
	numFound := 0
	for i := 0; i < len(w); i++ {
		wi := w[i]
		q := newQuery(t, wi.Tags)
		iter, _, err := s.FetchTaggedIDs(ContextWithDefaultTimeout(), ns,
			index.Query{Query: q},
			index.QueryOptions{
				StartInclusive: wi.Timestamp.Add(-1 * time.Second),
				EndExclusive:   wi.Timestamp.Add(1 * time.Second),
				SeriesLimit:    10,
			})
		if err != nil {
			if l := opts.Logger; l != nil {
				l.Error("fetch tagged IDs error", zap.Error(err))
			}
			continue
		}
		if !iter.Next() {
			if l := opts.Logger; l != nil {
				l.Warn("missing result",
					zap.String("queryID", wi.ID.String()),
					zap.ByteString("queryTags", consolidators.MustIdentTagIteratorToTags(wi.Tags, nil).ID()))
			}
			continue
		}
		cuNs, cuID, cuTag := iter.Current()
		if ns.String() != cuNs.String() {
			if l := opts.Logger; l != nil {
				l.Warn("namespace mismatch",
					zap.String("queryNamespace", ns.String()),
					zap.String("resultNamespace", cuNs.String()))
			}
			continue
		}
		if wi.ID.String() != cuID.String() {
			if l := opts.Logger; l != nil {
				l.Warn("id mismatch",
					zap.String("queryID", wi.ID.String()),
					zap.String("resultID", cuID.String()))
			}
			continue
		}
		if !ident.NewTagIterMatcher(wi.Tags).Matches(cuTag) {
			if l := opts.Logger; l != nil {
				l.Warn("tag mismatch",
					zap.ByteString("queryTags", consolidators.MustIdentTagIteratorToTags(wi.Tags, nil).ID()),
					zap.ByteString("resultTags", consolidators.MustIdentTagIteratorToTags(cuTag, nil).ID()))
			}
			continue
		}
		numFound++
	}
	return numFound
}

type TestIndexWrite struct {
	ID        ident.ID
	Tags      ident.TagIterator
	Timestamp xtime.UnixNano
	Value     float64
}

// GenerateTestIndexWrite generates test index writes.
func GenerateTestIndexWrite(periodID, numWrites, numTags int, startTime, endTime xtime.UnixNano) TestIndexWrites {
	writes := make([]TestIndexWrite, 0, numWrites)
	step := endTime.Sub(startTime) / time.Duration(numWrites+1)
	for i := 0; i < numWrites; i++ {
		id, tags := genIDTags(periodID, i, numTags)
		writes = append(writes, TestIndexWrite{
			ID:        id,
			Tags:      tags,
			Timestamp: startTime.Add(time.Duration(i) * step).Truncate(time.Second),
			Value:     float64(i),
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

func isIndexedChecked(
	t *testing.T,
	s client.Session,
	ns ident.ID,
	id ident.ID,
	tags ident.TagIterator,
) (bool, error) {
	return isIndexedCheckedWithTime(t, s, ns, id, tags, xtime.Now())
}

func isIndexedCheckedWithTime(
	t *testing.T,
	s client.Session,
	ns ident.ID,
	id ident.ID,
	tags ident.TagIterator,
	queryTime xtime.UnixNano,
) (bool, error) {
	q := newQuery(t, tags)
	iter, _, err := s.FetchTaggedIDs(ContextWithDefaultTimeout(), ns,
		index.Query{Query: q},
		index.QueryOptions{
			StartInclusive: queryTime,
			EndExclusive:   queryTime.Add(time.Nanosecond),
			SeriesLimit:    10,
		})
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

// ContextWithDefaultTimeout returns a context with a default timeout
// set of one minute.
func ContextWithDefaultTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Minute) //nolint
	return ctx
}
