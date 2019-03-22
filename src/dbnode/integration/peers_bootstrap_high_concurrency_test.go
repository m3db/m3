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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xlog.SimpleLogger
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	namesp, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().SetRetentionOptions(retentionOpts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp})

	batchSize := 16
	concurrency := 64
	setupOpts := []bootstrappableTestSetupOptions{
		{
			disablePeersBootstrapper: true,
		},
		{
			disablePeersBootstrapper:   false,
			bootstrapBlocksBatchSize:   batchSize,
			bootstrapBlocksConcurrency: concurrency,
		},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	total := 8 * batchSize * concurrency
	log.Debugf("testing a total of %d IDs with %d batch size %d concurrency", total, batchSize, concurrency)
	shardIDs := make([]string, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("id.%d", i)
		shardIDs = append(shardIDs, id)
	}

	now := setups[0].getNowFn()
	blockSize := retentionOpts.BlockSize()
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: shardIDs, NumPoints: 3, Start: now.Add(-3 * blockSize)},
		{IDs: shardIDs, NumPoints: 3, Start: now.Add(-2 * blockSize)},
		{IDs: shardIDs, NumPoints: 3, Start: now.Add(-blockSize)},
		{IDs: shardIDs, NumPoints: 3, Start: now},
	})
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, namesp.ID(), seriesMaps)
	}
}
