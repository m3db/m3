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
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

// TestBootstrapBeforeBufferRotationNoTick was added as a regression test after we identified
// an issue where blocks that were bootstrapped would sometimes be placed into series
// buffers that would not be drained before the next flush. Since the flush does not check
// for data in the buffer, if the buffers aren't drained then the bootstrapped data will
// be lose.
// A concrete example of this would be that a node with 2 hour block-size and 10 minute
// bufferPast goes down at 2:11PM. It turns back on and starts bootstrapping. There is
// an allocated series buffer bucket for the 12PM->2PM block, but it never gets drained
// because it doesn't have any data in it. While bootstrap is going on, a background tick
// is also occurring. The tick is 95% complete, and then the bootstrap completes and the
// blocks are all loaded into the series buffer. A small minority of series buffer buckets
// which hadn't already been ticked get drained, but the vast majority remain undrained.
// Once the tick finishes, the node begins flushing the 12PM->2PM block because the main
// requirements for performing a flush are as follows:
// 		1) currentTime > blockStart.Add(blockSize).Add(bufferPast)
// 		2) node is not bootstrapping (technically shard is not bootstrapping)
// 		3) at least one complete tick has occurred since blockStart.Add(blockSize).Add(bufferPast)
// This flush will lose a large portion of the bootstrapped data because its still stuck in
// the undrained series buffer 12PM->12PM bucket. As a result, it became clear that a four invariant
// needed to be maintained:
//		4) at least one complete tick has occurred since bootstrap completed (can be the same tick
// 		   that satisfies condition #3)
// This integration test ensures that we handle that situation properly by writing out
// a commitlog file that has a single write in the active block, then it starts the node
// and ensures that a tick starts before bootstrap begins and ends after bootstrap ends.
// We accomplish this by modifying the minimumTickFlushInterval runtime option.
// Finally, it verifies that the data from the commitlog is persisted and not lost.
func TestBootstrapBeforeBufferRotationNoTick(t *testing.T) {
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
	// to delay bootstrap completion until we've forced a tick to "hang". After the custom
	// test bootstrapper completes, the commitlog bootstrapper will run.
	bootstrapOpts := newDefaulTestResultOptions(setup.storageOpts)
	bootstrapCommitlogOpts := bcl.NewOptions().
		SetResultOptions(bootstrapOpts).
		SetCommitLogOptions(commitLogOpts)
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

	processOpts := bootstrap.NewProcessOptions().SetAdminClient(
		setup.m3dbAdminClient,
	)
	process := bootstrap.NewProcessProvider(test, processOpts, bootstrapOpts)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcessProvider(process)

	// Start a background goroutine which will wait until the server is started,
	// issue a single write into the active block, change the time to be far enough
	// into the *next* block that the series buffer for the previously active block
	// can be rotated, and then signals the test bootstrapper that it can proceed.
	go func() {
		// Wait for server to start
		setup.waitUntilServerIsUp()

		// Set the time such that the (previously) active block is ready to be flushed.
		now = now.Add(blockSize).Add(ropts.BufferPast()).Add(time.Second)
		setup.setNowFn(now)

		// Set the tick interval to be so large we can "hang" a tick at the end, preventing
		// it from completing until we're ready to "resume" it later.
		runtimeMgr := setup.storageOpts.RuntimeOptionsManager()
		existingOptions := runtimeMgr.Get()
		newOptions := existingOptions.SetTickMinimumInterval(2 * time.Hour)

		// Sleep for a multiple of the old tick interval to guarantee that the "hung" tick
		// that we will trigger after has a "tick start" time equal to the value of "now"
		// above.
		time.Sleep(existingOptions.TickMinimumInterval() * 10)

		// Update the runtime minimumTickInterval value
		err := runtimeMgr.Update(newOptions)
		if err != nil {
			panic(err)
		}

		// Sleep for a multiple of the old tick interval to guarantee that we enter a "hung"
		// tick.
		time.Sleep(existingOptions.TickMinimumInterval() * 10)

		// Twice because the test bootstrapper will need to run two times, once to fulfill
		// all historical blocks and once to fulfill the active block.
		signalCh <- struct{}{}
		signalCh <- struct{}{}
	}()
	require.NoError(t, setup.startServer()) // Blocks until bootstrap is complete

	// Now that bootstrapping has completed, re-enable ticking so that flushing can take place
	setup.mustSetTickMinimumInterval(100 * time.Millisecond)

	// Wait for a flush to complete
	setup.sleepFor10xTickMinimumInterval()

	defer func() {
		require.NoError(t, setup.stopServer())
	}()

	// Verify in-memory data match what we expect - commitlog write should not be lost
	expectedSeriesMaps := map[xtime.UnixNano]generate.SeriesBlock{
		xtime.ToUnixNano(commitlogWrite.Timestamp.Truncate(blockSize)): generate.SeriesBlock{
			generate.Series{
				ID:   testID,
				Data: []ts.Datapoint{commitlogWrite},
			},
		},
	}

	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], startTime, startTime.Add(blockSize))
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, expectedSeriesMaps, observedSeriesMaps)
}
