// +build integration
//
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

/*
 * This test runs the following situation, Now is 1p, data blockSize is 30m, index blockSize is 1h,
 * retention period 2h, buffer past 10mins, and buffer future 20mins. We write & index 50 metrics
 * between (1p, 1.30p).
 *
 * Then we move Now forward to 3p, and ensure the block is flushed. And data is still readable.
 */
func TestIndexBlockFlush(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		numWrites       = 50
		numTags         = 10
		retentionPeriod = 2 * time.Hour
		dataBlockSize   = 30 * time.Minute
		indexBlockSize  = time.Hour
		bufferFuture    = 20 * time.Minute
		bufferPast      = 10 * time.Minute
		verifyTimeout   = 2 * time.Minute
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

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(
		tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	testSetup.SetStorageOpts(testSetup.StorageOpts().SetInstrumentOptions(
		instrument.NewOptions().SetMetricsScope(scope)))

	t0 := time.Date(2018, time.May, 6, 13, 0, 0, 0, time.UTC)
	assert.True(t, t0.Equal(t0.Truncate(indexBlockSize)))
	t1 := t0.Add(20 * time.Minute)
	t2 := t0.Add(2 * time.Hour)
	testSetup.SetNowFn(t0)

	writesPeriod0 := GenerateTestIndexWrite(0, numWrites, numTags, t0, t1)

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	client := testSetup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)

	log.Info("starting data write")
	start := time.Now()
	writesPeriod0.Write(t, md.ID(), session)
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting till data is indexed")
	indexed := xclock.WaitUntil(func() bool {
		indexPeriod0 := writesPeriod0.NumIndexed(t, md.ID(), session)
		return indexPeriod0 == len(writesPeriod0)
	}, verifyTimeout)
	require.True(t, indexed)
	log.Info("verified data is indexed", zap.Duration("took", time.Since(start)))

	// "shared":"shared", is a common tag across all written metrics
	query := index.Query{
		Query: idx.NewTermQuery([]byte("shared"), []byte("shared"))}

	// ensure all data is present
	log.Info("querying period0 results")
	period0Results, _, err := session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	writesPeriod0.MatchesSeriesIters(t, period0Results)
	log.Info("found period0 results")

	// move time to 3p
	testSetup.SetNowFn(t2)

	// waiting till filesets found on disk
	log.Info("waiting till filesets found on disk")
	found := xclock.WaitUntil(func() bool {
		filesets, err := fs.IndexFileSetsAt(testSetup.FilePathPrefix(), md.ID(), t0)
		require.NoError(t, err)
		return len(filesets) == 1
	}, verifyTimeout)
	require.True(t, found)
	log.Info("found filesets found on disk")

	// ensure we've evicted the mutable segments
	log.Info("waiting till mutable segments are evicted")
	evicted := xclock.WaitUntil(func() bool {
		counters := reporter.Counters()
		counter, ok := counters["dbindex.blocks-evicted-mutable-segments"]
		return ok && counter > 0
	}, verifyTimeout)
	require.True(t, evicted)
	log.Info("mutable segments are evicted!")

	// ensure all data is still present
	log.Info("querying period0 results after flush")
	period0Results, _, err = session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	writesPeriod0.MatchesSeriesIters(t, period0Results)
	log.Info("found period0 results after flush")
}
