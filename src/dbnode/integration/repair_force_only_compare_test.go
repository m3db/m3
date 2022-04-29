//go:build integration
// +build integration

// Copyright (c) 2021 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/storage/repair"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestRepairForceAndOnlyCompare(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test both disjoint and shared series repair.
	genRepairData := genRepairDatafn(func(now xtime.UnixNano, blockSize time.Duration) (
		node0Data generate.SeriesBlocksByStart,
		node1Data generate.SeriesBlocksByStart,
		node2Data generate.SeriesBlocksByStart,
		allData generate.SeriesBlocksByStart,
	) {
		currBlockStart := now.Truncate(blockSize)
		node0Data = generate.BlocksByStart([]generate.BlockConfig{
			{IDs: []string{"foo"}, NumPoints: 90, Start: currBlockStart.Add(-4 * blockSize)},
			{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: currBlockStart.Add(-3 * blockSize)},
		})
		node1Data = generate.BlocksByStart([]generate.BlockConfig{
			{IDs: []string{"bar"}, NumPoints: 90, Start: currBlockStart.Add(-4 * blockSize)},
			{IDs: []string{"foo", "baz"}, NumPoints: 90, Start: currBlockStart.Add(-3 * blockSize)},
		})

		allData = make(map[xtime.UnixNano]generate.SeriesBlock)
		for start, data := range node0Data {
			allData[start] = append(allData[start], data...)
		}
		for start, data := range node1Data {
			allData[start] = append(allData[start], data...)
		}
		for start, data := range node2Data {
			allData[start] = append(allData[start], data...)
		}

		return node0Data, node1Data, node2Data, allData
	})

	// Test setups.
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	nsOpts := namespace.NewOptions().
		// Test needing to force enable repairs.
		SetRepairEnabled(false).
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
		{
			DisablePeersBootstrapper: true,
			EnableRepairs:            true,
			// Test forcing repair of type compare only repair.
			ForceRepairs: true,
			RepairType:   repair.OnlyCompareRepair,
		},
		{
			DisablePeersBootstrapper: true,
			EnableRepairs:            true,
			// Test forcing repair of type compare only repair.
			ForceRepairs: true,
			RepairType:   repair.OnlyCompareRepair,
		},
		{
			DisablePeersBootstrapper: true,
			EnableRepairs:            true,
			// Test forcing repair of type compare only repair.
			ForceRepairs: true,
			RepairType:   repair.OnlyCompareRepair,
		},
	}

	// nolint: govet
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Ensure that the current time is set such that the previous block is flushable.
	blockSize := retentionOpts.BlockSize()
	now := setups[0].NowFn()().Truncate(blockSize).Add(retentionOpts.BufferPast()).Add(time.Second)
	for _, setup := range setups {
		setup.SetNowFn(now)
	}

	node0Data, node1Data, node2Data, _ := genRepairData(now, blockSize)
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

	// Wait for repairs to occur at least once per node.
	log.Debug("waiting for repairs to run")
	var runSuccessPerNodeCounters []tally.CounterSnapshot
	require.True(t, waitUntil(func() bool {
		var successCounters []tally.CounterSnapshot
		for _, setup := range setups {
			scope := setup.Scope()
			for _, v := range scope.Snapshot().Counters() {
				if v.Name() != "repair.run" {
					continue
				}
				repairType, ok := v.Tags()["repair_type"]
				if !ok || repairType != "only_compare" {
					continue
				}
				if v.Value() > 0 {
					successCounters = append(successCounters, v)
					break
				}
			}
		}

		// Check if all counters are success.
		successAll := len(successCounters) == len(setups)
		if successAll {
			runSuccessPerNodeCounters = successCounters
			return true
		}
		return false
	}, 60*time.Second))

	// Verify that the repair runs only ran comparisons without repairing data.
	log.Debug("verifying repairs that ran")
	require.Equal(t, len(setups), len(runSuccessPerNodeCounters),
		"unexpected number of successful nodes ran repairs")
	for _, counter := range runSuccessPerNodeCounters {
		repairType, ok := counter.Tags()["repair_type"]
		require.True(t, ok)
		require.Equal(t, "only_compare", repairType)
		require.True(t, counter.Value() > 0)
	}

	// Verify data did not change (repair type is compare only).
	verifySeriesMaps(t, setups[0], namesp.ID(), node0Data)
	verifySeriesMaps(t, setups[1], namesp.ID(), node1Data)
	verifySeriesMaps(t, setups[2], namesp.ID(), node2Data)
}
