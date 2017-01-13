// +build integration

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

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestRepairHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setups
	log := xlog.SimpleLogger
	namesp := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
	opts := newTestOptions().
		SetNamespaces([]namespace.Metadata{namesp}).
		SetRepairInterval(3 * time.Second).
		SetRepairThrottle(1 * time.Second).
		SetRepairTimeJitter(0 * time.Second).
		SetNumShards(128)

	retentionOpts := retention.NewOptions().
		SetBufferDrain(3 * time.Second).
		SetRetentionPeriod(12 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	batchSize := 16
	concurrency := 64
	setupOpts := []multipleTestSetupsOptions{
		{
			disablePeersBootstrapper:          true,
			enableRepairer:                    true,
			fetchSeriesBlocksBatchSize:        batchSize,
			fetchSeriesBlocksBatchConcurrency: concurrency,
		},
		{
			disablePeersBootstrapper:          true,
			enableRepairer:                    true,
			fetchSeriesBlocksBatchSize:        batchSize,
			fetchSeriesBlocksBatchConcurrency: concurrency,
		},
	}
	setups, closeFn := newDefaultMultipleTestSetups(t, opts, retentionOpts, setupOpts)
	defer closeFn()

	// generate fake data
	total := 8 * batchSize * concurrency
	log.Debugf("testing a total of %d IDs with %d batch size %d concurrency", total, batchSize, concurrency)
	now := setups[0].getNowFn()
	shardIDs := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("id.%d", i)
		shardIDs = append(shardIDs, id)
	}
	blockSize := setups[0].storageOpts.RetentionOptions().BlockSize()
	seriesMaps := generateTestDataByStart([]testData{
		{ids: shardIDs, numPoints: 3, start: now.Add(-2 * blockSize)},
		{ids: shardIDs, numPoints: 3, start: now.Add(-blockSize)},
	})

	// split fake data into two halves
	splitMaps := splitSeriesMaps(seriesMaps, 2)
	require.NoError(t, writeTestDataToDisk(t, namesp.ID(), setups[0], splitMaps[0]))
	require.NoError(t, writeTestDataToDisk(t, namesp.ID(), setups[1], splitMaps[1]))
	log.Debug("fs bootstrap input data written to disk")

	// Move time forward to trigger repairs
	later := now.Add(blockSize * 2).Add(30 * time.Second)
	setups[0].setNowFn(later)
	setups[1].setNowFn(later)

	// Start the servers with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Wait an emperically determined amount of time for repairs to finish
	time.Sleep(120 * time.Second)

	// Stop the servers
	defer func() {
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	verifySeriesMaps(t, setups[0], namesp.ID(), seriesMaps)
	verifySeriesMaps(t, setups[1], namesp.ID(), seriesMaps)
}
