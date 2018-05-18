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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/topology"
	xclock "github.com/m3db/m3x/clock"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexSingleNodeHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		concurrency = 10
		writeEach   = 100
		numTags     = 10
	)

	levels := []topology.ReadConsistencyLevel{
		topology.ReadConsistencyLevelOne,
		topology.ReadConsistencyLevelUnstrictMajority,
		topology.ReadConsistencyLevelMajority,
		topology.ReadConsistencyLevelAll,
	}
	for _, lvl := range levels {
		t.Run(
			fmt.Sprintf("running test for %v", lvl),
			func(t *testing.T) {
				// Test setup
				md, err := namespace.NewMetadata(testNamespaces[0],
					namespace.NewOptions().
						SetRetentionOptions(defaultIntegrationTestRetentionOpts).
						SetCleanupEnabled(false).
						SetSnapshotEnabled(false).
						SetFlushEnabled(false).
						SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true)))
				require.NoError(t, err)

				testOpts := newTestOptions(t).
					SetNamespaces([]namespace.Metadata{md}).
					SetWriteNewSeriesAsync(true)
				testSetup, err := newTestSetup(t, testOpts, nil)
				require.NoError(t, err)
				defer testSetup.close()

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
					insertWg       sync.WaitGroup
					numTotalErrors uint32
				)
				now := testSetup.db.Options().ClockOptions().NowFn()()
				start := time.Now()
				log.Info("starting data write")

				for i := 0; i < concurrency; i++ {
					insertWg.Add(1)
					idx := i
					go func() {
						numErrors := uint32(0)
						for j := 0; j < writeEach; j++ {
							id, tags := genIDTags(idx, j, numTags)
							err := session.WriteTagged(md.ID(), id, tags, now, float64(1.0), xtime.Second, nil)
							if err != nil {
								numErrors++
							}
						}
						atomic.AddUint32(&numTotalErrors, numErrors)
						insertWg.Done()
					}()
				}

				insertWg.Wait()
				require.Zero(t, numTotalErrors)
				log.Infof("test data written in %v", time.Since(start))
				log.Infof("waiting to see if data is indexed")

				var (
					fetchWg sync.WaitGroup
				)
				for i := 0; i < concurrency; i++ {
					fetchWg.Add(1)
					idx := i
					go func() {
						id, tags := genIDTags(idx, writeEach-1, numTags)
						indexed := xclock.WaitUntil(func() bool {
							found := isIndexed(t, session, md.ID(), id, tags)
							return found
						}, 5*time.Second)
						assert.True(t, indexed)
						fetchWg.Done()
					}()
				}
				fetchWg.Wait()
				log.Infof("data is indexed in %v", time.Since(start))
			})
	}
}
