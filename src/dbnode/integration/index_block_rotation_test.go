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

	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/m3ninx/idx"
	xclock "github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
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

	testOpts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	t0 := time.Date(2018, time.May, 6, 13, 0, 0, 0, time.UTC)
	t1 := t0.Add(20 * time.Minute)
	t2 := t1.Add(3 * time.Hour)
	testSetup.setNowFn(t0)

	writesPeriod0 := generateTestIndexWrite(0, numWrites, numTags, t0, t1)

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
	log.Infof("test data written in %v", time.Since(start))

	log.Infof("waiting till data is indexed")
	indexed := xclock.WaitUntil(func() bool {
		indexPeriod0 := writesPeriod0.numIndexed(t, md.ID(), session)
		return indexPeriod0 == len(writesPeriod0)
	}, 5*time.Second)
	require.True(t, indexed)
	log.Infof("verifiied data is indexed in %v", time.Since(start))

	// "shared":"shared", is a common tag across all written metrics
	query := index.Query{
		idx.NewTermQuery([]byte("shared"), []byte("shared"))}

	// ensure all data is present
	log.Infof("querying period0 results")
	period0Results, _, err := session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	writesPeriod0.matchesSeriesIters(t, period0Results)
	log.Infof("found period0 results")

	// move time to 4p
	testSetup.setNowFn(t2)

	// give tick some time to evict the block
	testSetup.sleepFor10xTickMinimumInterval()

	// ensure all data is absent
	log.Infof("querying period0 results after expiry")
	period0Results, _, err = session.FetchTagged(
		md.ID(), query, index.QueryOptions{StartInclusive: t0, EndExclusive: t1})
	require.NoError(t, err)
	require.Equal(t, 0, period0Results.Len())
}
