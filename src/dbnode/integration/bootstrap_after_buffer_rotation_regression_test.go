// +build integration

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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

// TestBootstrapAfterBufferRotation was added as a regression test after we identified
// an issue where blocks that were bootstrapped would sometimes be placed into series
// buffers that had already been drained. Since we never drain a series buffer more than
// once, the bootstrapped blocks would either be lost, or over-write the data that was
// already in-memory instead of merging.
// This integration test ensures that we handle that situation properly by writing out
// a commitlog file that has a single write in the active block, then it starts the node
// and writes a single datapoint into the active block, and then triggers a buffer drain for
// that block by modifying the time to be after blockStart.Add(blockSize).Add(bufferPast).
// Once the buffer has been drained/rotated, we allow the bootstrap process to complete
// (controlled via a custom bootstrapper which can be delayed) and verify that the data
// from the commitlog and the data in-memory are properly merged together and neither is
// lost.
func TestBootstrapAfterBufferRotation(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		ropts = retention.NewOptions().
			SetRetentionPeriod(12 * time.Hour).
			SetBufferPast(5 * time.Minute).
			SetBufferFuture(5 * time.Minute)
		blockSize = ropts.BlockSize()
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ropts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetCommitLogBlockSize(blockSize).
		SetNamespaces([]namespace.Metadata{ns1})

	setup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.close()

	setup.mustSetTickMinimumInterval(100 * time.Millisecond)

	// Setup the commitlog and write a single datapoint into it one second into the
	// active block.
	commitLogOpts := setup.storageOpts.CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	testID := ident.StringID("foo")
	now := setup.getNowFn().Truncate(blockSize)
	setup.setNowFn(now)
	startTime := now
	commitlogWrite := ts.Datapoint{
		Timestamp: startTime.Add(time.Second),
		Value:     1,
	}
	seriesMaps := map[xtime.UnixNano]generate.SeriesBlock{
		xtime.ToUnixNano(startTime): generate.SeriesBlock{
			generate.Series{
				ID:   testID,
				Data: []ts.Datapoint{commitlogWrite},
			},
		},
	}
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, ns1, false)

	// Setup bootstrappers - We order them such that the custom test bootstrapper runs first
	// which does not bootstrap any data, but simply waits until it is signaled, allowing us
	// to delay bootstrap completion until after series buffer drain/rotation. After the custom
	// test bootstrapper completes, the commitlog bootstrapper will run.
	bootstrapOpts := newDefaulTestResultOptions(setup.storageOpts)
	bootstrapCommitlogOpts := bcl.NewOptions().
		SetResultOptions(bootstrapOpts).
		SetCommitLogOptions(commitLogOpts).
		SetRuntimeOptionsManager(runtime.NewOptionsManager())
	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
	commitlogBootstrapperProvider, err := bcl.NewCommitLogBootstrapperProvider(
		bootstrapCommitlogOpts, mustInspectFilesystem(fsOpts), nil)
	require.NoError(t, err)

	// Setup the test bootstrapper to only return success when a signal is sent.
	signalCh := make(chan struct{})
	bootstrapper, err := commitlogBootstrapperProvider.Provide()
	require.NoError(t, err)

	test := newTestBootstrapperSource(testBootstrapperSourceOptions{
		readData: func(
			_ namespace.Metadata,
			shardTimeRanges result.ShardTimeRanges,
			_ bootstrap.RunOptions,
		) (result.DataBootstrapResult, error) {
			<-signalCh
			result := result.NewDataBootstrapResult()
			// Mark all as unfulfilled so the commitlog bootstrapper will be called after
			result.SetUnfulfilled(shardTimeRanges)
			return result, nil
		},
	}, bootstrapOpts, bootstrapper)

	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(setup).
		SetOrigin(setup.origin)

	processProvider, err := bootstrap.NewProcessProvider(
		test, processOpts, bootstrapOpts)
	require.NoError(t, err)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcessProvider(processProvider)

	// Start a background goroutine which will wait until the server is started,
	// issue a single write into the active block, change the time to be far enough
	// into the *next* block that the series buffer for the previously active block
	// can be rotated, and then signals the test bootstrapper that it can proceed.
	var memoryWrite ts.Datapoint
	go func() {
		// Wait for server to start
		setup.waitUntilServerIsUp()
		now = now.Add(blockSize)
		setup.setNowFn(now)
		memoryWrite = ts.Datapoint{
			Timestamp: now.Add(-10 * time.Second),
			Value:     2,
		}

		// Issue the write (still in the same block as the commitlog write).
		err := setup.writeBatch(ns1.ID(), generate.SeriesBlock{
			generate.Series{
				ID:   ident.StringID("foo"),
				Data: []ts.Datapoint{memoryWrite},
			}})
		if err != nil {
			panic(err)
		}

		// Change the time far enough into the next block that a series buffer
		// rotation will occur for the previously active block.
		now = now.Add(ropts.BufferPast()).Add(time.Second)
		setup.setNowFn(now)
		setup.sleepFor10xTickMinimumInterval()

		// Twice because the test bootstrapper will need to run two times, once to fulfill
		// all historical blocks and once to fulfill the active block.
		signalCh <- struct{}{}
		signalCh <- struct{}{}
	}()
	require.NoError(t, setup.startServer()) // Blocks until bootstrap is complete

	defer func() {
		require.NoError(t, setup.stopServer())
	}()

	// Verify in-memory data match what we expect - both commitlog and memory write
	// should be present.
	expectedSeriesMaps := map[xtime.UnixNano]generate.SeriesBlock{
		xtime.ToUnixNano(commitlogWrite.Timestamp.Truncate(blockSize)): generate.SeriesBlock{
			generate.Series{
				ID:   testID,
				Data: []ts.Datapoint{commitlogWrite, memoryWrite},
			},
		},
	}

	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], startTime, startTime.Add(blockSize))
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, expectedSeriesMaps, observedSeriesMaps)
}
