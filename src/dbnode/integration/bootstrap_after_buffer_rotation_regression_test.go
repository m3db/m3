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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

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
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	setup.MustSetTickMinimumInterval(100 * time.Millisecond)

	// Setup the commitlog and write a single datapoint into it one second into the
	// active block.
	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	testID := ident.StringID("foo")
	now := setup.NowFn()().Truncate(blockSize)
	setup.SetNowFn(now)
	startTime := now
	commitlogWrite := ts.Datapoint{
		TimestampNanos: startTime.Add(time.Second),
		Value:          1,
	}
	seriesMaps := map[xtime.UnixNano]generate.SeriesBlock{
		startTime: {
			generate.Series{
				ID:   testID,
				Data: []generate.TestValue{{Datapoint: commitlogWrite}},
			},
		},
	}
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, ns1, false)

	// Setup bootstrappers - We order them such that the custom test bootstrapper runs first
	// which does not bootstrap any data, but simply waits until it is signaled, allowing us
	// to delay bootstrap completion until after series buffer drain/rotation. After the custom
	// test bootstrapper completes, the commitlog bootstrapper will run.
	bootstrapOpts := newDefaulTestResultOptions(setup.StorageOpts())
	bootstrapCommitlogOpts := bcl.NewOptions().
		SetResultOptions(bootstrapOpts).
		SetCommitLogOptions(commitLogOpts).
		SetRuntimeOptionsManager(runtime.NewOptionsManager())
	fsOpts := setup.StorageOpts().CommitLogOptions().FilesystemOptions()
	commitlogBootstrapperProvider, err := bcl.NewCommitLogBootstrapperProvider(
		bootstrapCommitlogOpts, mustInspectFilesystem(fsOpts), nil)
	require.NoError(t, err)

	// Setup the test bootstrapper to only return success when a signal is sent.
	signalCh := make(chan struct{})
	bs, err := commitlogBootstrapperProvider.Provide()
	require.NoError(t, err)

	test := newTestBootstrapperSource(testBootstrapperSourceOptions{
		read: func(
			ctx context.Context,
			namespaces bootstrap.Namespaces,
			cache bootstrap.Cache,
		) (bootstrap.NamespaceResults, error) {
			<-signalCh
			// Mark all as unfulfilled so the commitlog bootstrapper will be called after
			noopNone := bootstrapper.NewNoOpNoneBootstrapperProvider()
			bs, err := noopNone.Provide()
			if err != nil {
				return bootstrap.NamespaceResults{}, err
			}
			return bs.Bootstrap(ctx, namespaces, cache)
		},
	}, bootstrapOpts, bs)

	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(setup).
		SetOrigin(setup.Origin())

	processProvider, err := bootstrap.NewProcessProvider(
		test, processOpts, bootstrapOpts, fsOpts)
	require.NoError(t, err)
	setup.SetStorageOpts(setup.StorageOpts().SetBootstrapProcessProvider(processProvider))

	// Start a background goroutine which will wait until the server is started,
	// issue a single write into the active block, change the time to be far enough
	// into the *next* block that the series buffer for the previously active block
	// can be rotated, and then signals the test bootstrapper that it can proceed.
	var memoryWrite ts.Datapoint
	go func() {
		// Wait for server to start
		setup.WaitUntilServerIsUp()
		now = now.Add(blockSize)
		setup.SetNowFn(now)
		memoryWrite = ts.Datapoint{
			TimestampNanos: now.Add(-10 * time.Second),
			Value:          2,
		}

		// Issue the write (still in the same block as the commitlog write).
		err := setup.WriteBatch(ns1.ID(), generate.SeriesBlock{
			generate.Series{
				ID:   ident.StringID("foo"),
				Data: []generate.TestValue{{Datapoint: memoryWrite}},
			}})
		if err != nil {
			panic(err)
		}

		// Change the time far enough into the next block that a series buffer
		// rotation will occur for the previously active block.
		now = now.Add(ropts.BufferPast()).Add(time.Second)
		setup.SetNowFn(now)
		setup.SleepFor10xTickMinimumInterval()

		// Close signalCh to unblock bootstrapper and run the bootstrap till the end
		close(signalCh)
	}()
	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete

	defer func() {
		require.NoError(t, setup.StopServer())
	}()

	// Verify in-memory data match what we expect - both commitlog and memory write
	// should be present.
	expectedSeriesMaps := map[xtime.UnixNano]generate.SeriesBlock{
		commitlogWrite.TimestampNanos.Truncate(blockSize): {
			generate.Series{
				ID:   testID,
				Data: []generate.TestValue{{Datapoint: commitlogWrite}, {Datapoint: memoryWrite}},
			},
		},
	}

	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], startTime, startTime.Add(blockSize))
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, expectedSeriesMaps, observedSeriesMaps)
}
