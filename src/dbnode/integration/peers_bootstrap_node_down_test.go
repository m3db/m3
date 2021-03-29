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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapNodeDown(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)

	namesp, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().SetRetentionOptions(retentionOpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	setupOpts := []BootstrappableTestSetupOptions{
		{DisablePeersBootstrapper: true},
		{DisablePeersBootstrapper: true},
		{DisablePeersBootstrapper: false},
	}
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	now := setups[0].NowFn()()
	blockSize := retentionOpts.BlockSize()
	// Make sure we have multiple blocks of data for multiple series to exercise
	// the grouping and aggregating logic in the client peer bootstrapping process
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-4 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-3 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-2 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: now},
	})
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps, 0)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].StartServer())

	// Leave second node down, start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[2].StartServer())
	log.Debug("first and third servers are now up")

	// Stop the servers
	defer func() {
		testSetups{setups[0], setups[2]}.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	expect := testSetups{setups[0], setups[2]}
	for _, setup := range expect {
		verifySeriesMaps(t, setup, namesp.ID(), seriesMaps)
	}
}
