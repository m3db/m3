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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	xclock "github.com/m3db/m3/src/x/clock"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func TestIndexSingleNodeHighConcurrencyStandard(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrency: 8,
		writeEach:   100,
		numTags:     10,
	})
}

var testScope tally.TestScope

func TestMain(m *testing.M) {
	exitVal := m.Run()
	fmt.Println("test main exit")
	if testScope != nil {
		for k, v := range testScope.Snapshot().Counters() {
			fmt.Printf("%s=%v\n", k, v.Value())
		}
	}

	os.Exit(exitVal)
}

func TestIndexSingleNodeHighConcurrencyHighCardinality(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	testIndexSingleNodeHighConcurrency(t, testIndexHighConcurrencyOptions{
		concurrency: 8,
		writeEach:   50000,
		numTags:     2,
	})
}

type testIndexHighConcurrencyOptions struct {
	concurrency int
	writeEach   int
	numTags     int
}

func testIndexSingleNodeHighConcurrency(
	t *testing.T,
	opts testIndexHighConcurrencyOptions,
) {
	// Test setup
	md, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().
			SetRetentionOptions(defaultIntegrationTestRetentionOpts).
			SetCleanupEnabled(false).
			SetSnapshotEnabled(false).
			SetFlushEnabled(false).
			SetColdWritesEnabled(true).
			SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true)))
	require.NoError(t, err)

	testOpts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true).
		// Use default time functions (server time not frozen).
		SetNowFn(time.Now)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	testScope = testSetup.scope

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

	var (
		insertWg        sync.WaitGroup
		numTotalErrors  = atomic.NewUint32(0)
		numTotalSuccess = atomic.NewUint32(0)
	)
	nowFn := testSetup.db.Options().ClockOptions().NowFn()
	start := time.Now()
	log.Info("starting data write",
		zap.Time("serverTime", nowFn()))

	workerPool := xsync.NewWorkerPool(5000)
	workerPool.Init()

	for i := 0; i < opts.concurrency; i++ {
		idx := i
		insertWg.Add(1)
		go func() {
			defer insertWg.Done()

			for j := 0; j < opts.writeEach; j++ {
				j := j
				insertWg.Add(1)
				workerPool.Go(func() {
					defer insertWg.Done()

					id, tags := genIDTags(idx, j, opts.numTags)
					now := time.Now()

					for k := 0; k < 2; k++ {
						if k == 1 {
							now = now.Add(-4 * time.Hour)
						}

						err := session.WriteTagged(md.ID(), id, tags, now,
							float64(j), xtime.Second, nil)
						if err != nil {
							if v := numTotalErrors.Inc(); (v-1)%100 == 0 && (v-1) < 1000 {
								log.Error("err writing", zap.Error(err))
							}
						} else {
							numTotalSuccess.Inc()
						}
					}

				})
			}
		}()
	}

	insertWg.Wait()

	require.Equal(t, int(0), int(numTotalErrors.Load()))

	log.Info("test data written",
		zap.Duration("took", time.Since(start)),
		zap.Int("written", int(numTotalSuccess.Load())),
		zap.Time("serverTime", nowFn()))

	log.Info("waiting to see if data is indexed")

	var (
		fetchWg sync.WaitGroup
	)
	for i := 0; i < opts.concurrency; i++ {
		fetchWg.Add(1)
		idx := i
		go func() {
			id, tags := genIDTags(idx, opts.writeEach-1, opts.numTags)
			indexed := xclock.WaitUntil(func() bool {
				found := isIndexed(t, session, md.ID(), id, tags)
				return found
			}, 30*time.Second)
			assert.True(t, indexed)
			fetchWg.Done()
		}()
	}
	fetchWg.Wait()
	log.Info("data is indexed", zap.Duration("took", time.Since(start)))
}
