// +build integration
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

/*
 * This test runs the following situation, Now is 1p, data blockSize is 1h, index blockSize is 2h,
 * retention period 2h, buffer past 10mins, and buffer future 20mins. We write & index 50 metrics
 * between (12p, 12.50p). We then ensure that index writes within this warm index start -> start of buffer
 * past gap are indexed.
 *
 */
func TestWarmIndexWriteGap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		numWrites       = 50
		numTags         = 10
		retentionPeriod = 2 * time.Hour
		dataBlockSize   = time.Hour
		indexBlockSize  = 2 * time.Hour
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
					SetBlockSize(indexBlockSize).SetEnabled(true)).
			SetColdWritesEnabled(true))
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	t0 := time.Date(2018, time.May, 6, 13, 0, 0, 0, time.UTC)
	// Issue writes in the gap between warm index start and start of buffer past.
	t1 := t0.Truncate(indexBlockSize)
	t2 := t0.Truncate(dataBlockSize).Add(-bufferPast)
	testSetup.SetNowFn(t0)

	writesPeriod0 := GenerateTestIndexWrite(0, numWrites, numTags, t1, t2)

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
		md.ID(), query, index.QueryOptions{StartInclusive: t1, EndExclusive: t2})
	require.NoError(t, err)
	writesPeriod0.MatchesSeriesIters(t, period0Results)
	log.Info("found period0 results")
}
