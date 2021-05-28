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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

/*
 * This test runs the following situation, Now is 1p, data blockSize is 30m, index blockSize is 1h,
 * retention period 2h, buffer past 10mins, and buffer future 20mins. We write & index 50 metrics
 * between (1p, 1.30p).
 *
 * Then we move Now forward to 4p, and ensure the block is evicted.
 */
func TestIndexBlockRotation(t *testing.T) {
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

	t0 := xtime.ToUnixNano(time.Date(2018, time.May, 6, 13, 0, 0, 0, time.UTC))
	t1 := t0.Add(20 * time.Minute)
	t2 := t1.Add(3 * time.Hour)
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
	}, 5*time.Second)
	require.True(t, indexed)
	log.Info("verifiied data is indexed", zap.Duration("took", time.Since(start)))

	// "shared":"shared", is a common tag across all written metrics
	query := index.Query{
		Query: idx.NewTermQuery([]byte("shared"), []byte("shared"))}

	// ensure all data is present
	log.Info("querying period0 results")
	period0Results, _, err := session.FetchTagged(ContextWithDefaultTimeout(),
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	writesPeriod0.MatchesSeriesIters(t, period0Results)
	log.Info("found period0 results")

	// move time to 4p
	testSetup.SetNowFn(t2)

	// give tick some time to evict the block
	testSetup.SleepFor10xTickMinimumInterval()

	// ensure all data is absent
	log.Info("querying period0 results after expiry")
	period0Results, _, err = session.FetchTagged(ContextWithDefaultTimeout(),
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)

	if period0Results.Len() != 0 {
		// sometimes eviction lags behind; give tick some extra time if eviction
		// hasn't completed yet.
		testSetup.SleepFor10xTickMinimumInterval()

		period0Results, _, err = session.FetchTagged(ContextWithDefaultTimeout(),
			md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
		require.NoError(t, err)
	}

	require.Equal(t, 0, period0Results.Len())
}
