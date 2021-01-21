// +build integration

// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestRepairDisjointSeries(t *testing.T) {
	genRepairData := func(now time.Time, blockSize time.Duration) (
		node0Data generate.SeriesBlocksByStart,
		node1Data generate.SeriesBlocksByStart,
		node2Data generate.SeriesBlocksByStart,
		allData generate.SeriesBlocksByStart,
	) {
		currBlockStart := now.Truncate(blockSize)
		node0Data = generate.BlocksByStart([]generate.BlockConfig{
			{IDs: []string{"foo"}, NumPoints: 90, Start: currBlockStart.Add(-4 * blockSize)},
		})
		node1Data = generate.BlocksByStart([]generate.BlockConfig{
			{IDs: []string{"bar"}, NumPoints: 90, Start: currBlockStart.Add(-4 * blockSize)},
		})

		allData = make(map[xtime.UnixNano]generate.SeriesBlock)
		for start, data := range node0Data {
			for _, series := range data {
				allData[start] = append(allData[start], series)
			}
		}
		for start, data := range node1Data {
			for _, series := range data {
				allData[start] = append(allData[start], series)
			}
		}
		for start, data := range node2Data {
			for _, series := range data {
				allData[start] = append(allData[start], series)
			}
		}

		return node0Data, node1Data, node2Data, allData
	}

	testRepair(t, genRepairData, testRepairOptions{})
}

func TestRepairMergeSeries(t *testing.T) {
	genRepairData := func(now time.Time, blockSize time.Duration) (
		node0Data generate.SeriesBlocksByStart,
		node1Data generate.SeriesBlocksByStart,
		node2Data generate.SeriesBlocksByStart,
		allData generate.SeriesBlocksByStart,
	) {
		currBlockStart := now.Truncate(blockSize)
		allData = generate.BlocksByStart([]generate.BlockConfig{
			{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: currBlockStart.Add(-4 * blockSize)},
			{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: currBlockStart.Add(-3 * blockSize)},
			{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: currBlockStart.Add(-2 * blockSize)}})
		node0Data = make(map[xtime.UnixNano]generate.SeriesBlock)
		node1Data = make(map[xtime.UnixNano]generate.SeriesBlock)

		remainder := 0
		appendSeries := func(target map[xtime.UnixNano]generate.SeriesBlock, start time.Time, s generate.Series) {
			var dataWithMissing []generate.TestValue
			for i := range s.Data {
				if i%2 != remainder {
					continue
				}
				dataWithMissing = append(dataWithMissing, s.Data[i])
			}
			target[xtime.ToUnixNano(start)] = append(
				target[xtime.ToUnixNano(start)],
				generate.Series{ID: s.ID, Data: dataWithMissing},
			)
			remainder = 1 - remainder
		}
		for start, data := range allData {
			for _, series := range data {
				appendSeries(node0Data, start.ToTime(), series)
				appendSeries(node1Data, start.ToTime(), series)
			}
		}

		return node0Data, node1Data, node2Data, allData
	}

	testRepair(t, genRepairData, testRepairOptions{})
}

func TestRepairDoesNotRepairCurrentBlock(t *testing.T) {
	genRepairData := func(now time.Time, blockSize time.Duration) (
		node0Data generate.SeriesBlocksByStart,
		node1Data generate.SeriesBlocksByStart,
		node2Data generate.SeriesBlocksByStart,
		allData generate.SeriesBlocksByStart,
	) {
		currBlockStart := now.Truncate(blockSize)
		node0Data = generate.BlocksByStart([]generate.BlockConfig{
			// Write in previous block should be repaired.
			{IDs: []string{"prevBlock1", "prevBlock2"}, NumPoints: 1, Start: currBlockStart.Add(-blockSize)},
			// Write in current block, should not be repaired.
			{IDs: []string{"currBlock1", "currBlock2"}, NumPoints: 1, Start: currBlockStart},
		})

		allData = make(map[xtime.UnixNano]generate.SeriesBlock)
		for start, data := range node0Data {
			if !start.ToTime().Equal(currBlockStart) {
				allData[start] = data
			}
		}
		require.Equal(t, 1, len(allData))

		return node0Data, node1Data, node2Data, allData
	}

	currBlockSeries := []ident.ID{ident.StringID("currBlock1"), ident.StringID("currBlock2")}
	testRepairOpts := testRepairOptions{
		node1ShouldNotContainSeries: currBlockSeries,
		node2ShouldNotContainSeries: currBlockSeries}
	testRepair(t, genRepairData, testRepairOpts)
}

type genRepairDatafn func(
	now time.Time,
	blockSize time.Duration,
) (
	node0Data generate.SeriesBlocksByStart,
	node1Data generate.SeriesBlocksByStart,
	node2Data generate.SeriesBlocksByStart,
	allData generate.SeriesBlocksByStart)

type testRepairOptions struct {
	node0ShouldNotContainSeries []ident.ID
	node1ShouldNotContainSeries []ident.ID
	node2ShouldNotContainSeries []ident.ID
}

func testRepair(
	t *testing.T,
	genRepairData genRepairDatafn,
	testRepairOpts testRepairOptions,
) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups.
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(true).
		// Explicitly ensure that the repair feature works even if cold writes is disabled
		// at the namespace level.
		SetColdWritesEnabled(false).
		SetRetentionOptions(retentionOpts)
	namesp, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	setupOpts := []BootstrappableTestSetupOptions{
		{DisablePeersBootstrapper: true, EnableRepairs: true},
		{DisablePeersBootstrapper: true, EnableRepairs: true},
		{DisablePeersBootstrapper: true, EnableRepairs: true},
	}
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Ensure that the current time is set such that the previous block is flushable.
	blockSize := retentionOpts.BlockSize()
	now := setups[0].NowFn()().Truncate(blockSize).Add(retentionOpts.BufferPast()).Add(time.Second)
	for _, setup := range setups {
		setup.SetNowFn(now)
	}

	node0Data, node1Data, node2Data, allData := genRepairData(now, blockSize)
	if node0Data != nil {
		require.NoError(t, writeTestDataToDisk(namesp, setups[0], node0Data, 0))
	}
	if node1Data != nil {
		require.NoError(t, writeTestDataToDisk(namesp, setups[1], node1Data, 0))
	}
	if node2Data != nil {
		require.NoError(t, writeTestDataToDisk(namesp, setups[2], node2Data, 0))
	}

	// Start the servers with filesystem bootstrappers.
	setups.parallel(func(s TestSetup) {
		if err := s.StartServer(); err != nil {
			panic(err)
		}
	})
	log.Debug("servers are now up")

	// Stop the servers.
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	require.True(t, waitUntil(func() bool {
		for _, setup := range setups {
			if err := checkFlushedDataFiles(setup.ShardSet(), setup.StorageOpts(), namesp.ID(), allData); err != nil {
				// Increment the time each time it fails to make sure background processes are able to proceed.
				for _, s := range setups {
					s.SetNowFn(s.NowFn()().Add(time.Millisecond))
				}
				return false
			}
		}
		return true
	}, 60*time.Second))

	// Verify in-memory data matches what we expect.
	verifySeriesMaps(t, setups[0], namesp.ID(), allData)
	verifySeriesMaps(t, setups[1], namesp.ID(), allData)
	verifySeriesMaps(t, setups[2], namesp.ID(), allData)

	for _, seriesID := range testRepairOpts.node0ShouldNotContainSeries {
		contains, err := containsSeries(setups[0], namesp.ID(), seriesID, now.Add(-retentionOpts.RetentionPeriod()), now)
		require.NoError(t, err)
		require.False(t, contains)
	}
	for _, seriesID := range testRepairOpts.node1ShouldNotContainSeries {
		contains, err := containsSeries(setups[1], namesp.ID(), seriesID, now.Add(-retentionOpts.RetentionPeriod()), now)
		require.NoError(t, err)
		require.False(t, contains)
	}
	for _, seriesID := range testRepairOpts.node2ShouldNotContainSeries {
		contains, err := containsSeries(setups[2], namesp.ID(), seriesID, now.Add(-retentionOpts.RetentionPeriod()), now)
		require.NoError(t, err)
		require.False(t, contains)
	}
}
