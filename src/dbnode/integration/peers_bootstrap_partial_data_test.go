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
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

// This test simulates a case where node fails / reboots while fetching data from peers.
// When restarting / retrying bootstrap process, there already will be some data on disk,
// which can be fulfilled by filesystem bootstrapper.
func TestPeersBootstrapPartialData(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)
	blockSize := 2 * time.Hour
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(5 * blockSize).
		SetBlockSize(blockSize).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	idxOpts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(blockSize)
	nsOpts := namespace.NewOptions().SetRetentionOptions(retentionOpts).SetIndexOptions(idxOpts)
	namesp, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	setupOpts := []BootstrappableTestSetupOptions{
		{DisablePeersBootstrapper: true},
		{
			DisableCommitLogBootstrapper: true,
			DisablePeersBootstrapper:     false,
		},
	}
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data to first node
	now := setups[0].NowFn()()
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-5 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-3 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-2 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now},
	}
	seriesMaps := generate.BlocksByStart(inputData)
	require.NoError(t, writeTestDataToDisk(namesp, setups[0], seriesMaps, 0))

	// Write a subset of blocks to second node, simulating an incomplete peer bootstrap.
	partialBlockStarts := map[xtime.UnixNano]struct{}{
		xtime.ToUnixNano(inputData[0].Start): {},
		xtime.ToUnixNano(inputData[1].Start): {},
		xtime.ToUnixNano(inputData[2].Start): {},
	}
	partialSeriesMaps := make(generate.SeriesBlocksByStart)
	for blockStart, series := range seriesMaps {
		if _, ok := partialBlockStarts[blockStart]; ok {
			partialSeriesMaps[blockStart] = series
		}
	}
	require.NoError(t, writeTestDataToDisk(namesp, setups[1], partialSeriesMaps, 0,
		func(gOpts generate.Options) generate.Options {
			return gOpts.SetWriteEmptyShards(false)
		}))

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].StartServer())

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].StartServer())
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, namesp.ID(), seriesMaps)
	}
}
