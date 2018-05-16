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
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3ninx/idx"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

/*
 * This test runs the following situation, Now is 1p, data blockSize is 30m, index blockSize is 1h,
 * retention period 2h, buffer past 10mins, and buffer future 5mins. We write & index 50 metrics
 * between (12.50p,1p) and 50 metrics between (1p, 1.05p).
 *
 * Then we query for data for three periods: (12.50,1p), (1p,1.05p) and (12.50,1.05p); and ensure
 * the data we're returned is valid.
 */
func TestIndexMultipleBlockQuery(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		numWrites       = 50
		numTags         = 10
		retentionPeriod = 2 * time.Hour
		dataBlockSize   = 30 * time.Minute
		indexBlockSize  = time.Hour
		bufferFuture    = 5 * time.Minute
		bufferPast      = 10 * time.Minute
	)

	// Test setup
	md, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().
			SetRetentionOptions(
				retention.NewOptions().
					SetRetentionPeriod(retentionPeriod).
					SetBufferPast(bufferPast).
					SetBufferFuture(bufferFuture).
					SetBlockSize(dataBlockSize)).
			SetIndexOptions(
				namespace.NewIndexOptions().
					SetBlockSize(indexBlockSize).SetEnabled(true)))
	require.NoError(t, err)

	testOpts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	t0 := time.Date(2018, time.May, 6, 12, 50, 0, 0, time.UTC)
	t1 := t0.Add(10 * time.Minute)
	t2 := t1.Add(5 * time.Minute)
	testSetup.setNowFn(t1)

	writesPeriod0 := generateTestIndexWrite(0, numWrites, numTags, t0, t1)
	writesPeriod1 := generateTestIndexWrite(1, numWrites, numTags, t1, t2)

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.startServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	client := testSetup.m3dbClient
	session, err := client.DefaultSession()
	require.NoError(t, err)

	log.Info("starting data write")
	start := time.Now()
	writesPeriod0.write(t, md.ID(), session)
	writesPeriod1.write(t, md.ID(), session)
	log.Infof("test data written in %v", time.Since(start))

	log.Infof("waiting till data is indexed")
	indexed := xclock.WaitUntil(func() bool {
		indexPeriod0 := writesPeriod0.numIndexed(t, md.ID(), session)
		indexPeriod1 := writesPeriod1.numIndexed(t, md.ID(), session)
		return indexPeriod0 == len(writesPeriod0) &&
			indexPeriod1 == len(writesPeriod1)
	}, 5*time.Second)
	require.True(t, indexed)
	log.Infof("verifiied data is indexed in %v", time.Since(start))

	// "shared":"shared", is a common tag across all written metrics
	query := index.Query{
		idx.NewTermQuery([]byte("shared"), []byte("shared"))}

	log.Infof("querying period0 results")
	period0Results, _, err := session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	writesPeriod0.matchesSeriesIters(t, period0Results)
	log.Infof("found period0 results")

	log.Infof("querying period1 results")
	period1Results, _, err := session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t1, EndExclusive: t2})
	require.NoError(t, err)
	writesPeriod1.matchesSeriesIters(t, period1Results)
	log.Infof("found period1 results")

	log.Infof("querying period 0+1 results")
	period01Results, _, err := session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t2})
	require.NoError(t, err)
	writes := append(writesPeriod0, writesPeriod1...)
	writes.matchesSeriesIters(t, period01Results)
	log.Infof("found period 0+1 results")
}

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
