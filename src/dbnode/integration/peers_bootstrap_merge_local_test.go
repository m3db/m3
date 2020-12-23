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
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapMergeLocal(t *testing.T) {
	testPeersBootstrapMergeLocal(t, nil, nil)
}

func TestProtoPeersBootstrapMergeLocal(t *testing.T) {
	testPeersBootstrapMergeLocal(t, setProtoTestOptions, setProtoTestInputConfig)
}

func testPeersBootstrapMergeLocal(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	namesp, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().SetRetentionOptions(retentionOpts))
	require.NoError(t, err)

	var (
		opts = NewTestOptions(t).
			SetNamespaces([]namespace.Metadata{namesp}).
			// Use TChannel clients for writing / reading because we want to target individual nodes at a time
			// and not write/read all nodes in the cluster.
			SetUseTChannelClientForWriting(true).
			SetUseTChannelClientForReading(true)

		reporter = xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())

		// Enable useTchannelClientForWriting because this test relies upon being
		// able to write data to a single node, and the M3DB client does not support
		// that, but we can accomplish it by using an individual nodes TChannel endpoints.
		setupOpts = []BootstrappableTestSetupOptions{
			{
				DisablePeersBootstrapper:    true,
				UseTChannelClientForWriting: true,
			},
			{
				DisablePeersBootstrapper:    false,
				UseTChannelClientForWriting: true,
				TestStatsReporter:           reporter,
			},
		}
	)

	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		namesp = opts.Namespaces()[0]
	}

	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node, ensure to overflow past
	now := setups[0].NowFn()()
	cutoverAt := now.Add(retentionOpts.BufferFuture())
	completeAt := now.Add(180 * time.Second)
	blockSize := retentionOpts.BlockSize()
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 180, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: int(completeAt.Sub(now) / time.Second), Start: now},
	}
	if updateInputConfig != nil {
		updateInputConfig(inputData)
	}
	seriesMaps := generate.BlocksByStart(inputData)
	firstNodeSeriesMaps := map[xtime.UnixNano]generate.SeriesBlock{}
	directWritesSeriesMaps := map[xtime.UnixNano]generate.SeriesBlock{}
	for start, s := range seriesMaps {
		for i := range s {
			isPartialSeries := start.ToTime().Equal(now)
			if !isPartialSeries {
				// Normal series should just be straight up copied from first node
				firstNodeSeriesMaps[start] = append(firstNodeSeriesMaps[start], s[i])
				continue
			}

			firstNodeSeries := generate.Series{ID: s[i].ID}
			directWritesSeries := generate.Series{ID: s[i].ID}
			for j := range s[i].Data {
				if s[i].Data[j].Timestamp.Before(cutoverAt) {
					// If partial series and before cutover then splice between first node and second node
					if j%2 == 0 {
						firstNodeSeries.Data = append(firstNodeSeries.Data, s[i].Data[j])
						continue
					}
					directWritesSeries.Data = append(directWritesSeries.Data, s[i].Data[j])
					continue
				}
				// If after cutover just use as writes directly to the second node
				directWritesSeries.Data = append(directWritesSeries.Data, s[i].Data[j])
			}

			firstNodeSeriesMaps[start] = append(firstNodeSeriesMaps[start], firstNodeSeries)
			directWritesSeriesMaps[start] = append(directWritesSeriesMaps[start], directWritesSeries)
		}
	}

	// Assert test data for first node is correct
	require.Equal(t, 2, len(firstNodeSeriesMaps))

	require.Equal(t, 2, firstNodeSeriesMaps[xtime.ToUnixNano(now.Add(-blockSize))].Len())
	require.Equal(t, "foo", firstNodeSeriesMaps[xtime.ToUnixNano(now.Add(-blockSize))][0].ID.String())
	require.Equal(t, 180, len(firstNodeSeriesMaps[xtime.ToUnixNano(now.Add(-blockSize))][0].Data))
	require.Equal(t, "bar", firstNodeSeriesMaps[xtime.ToUnixNano(now.Add(-blockSize))][1].ID.String())
	require.Equal(t, 180, len(firstNodeSeriesMaps[xtime.ToUnixNano(now.Add(-blockSize))][1].Data))

	require.Equal(t, 2, firstNodeSeriesMaps[xtime.ToUnixNano(now)].Len())
	require.Equal(t, "foo", firstNodeSeriesMaps[xtime.ToUnixNano(now)][0].ID.String())
	require.Equal(t, 60, len(firstNodeSeriesMaps[xtime.ToUnixNano(now)][0].Data))
	require.Equal(t, "baz", firstNodeSeriesMaps[xtime.ToUnixNano(now)][1].ID.String())
	require.Equal(t, 60, len(firstNodeSeriesMaps[xtime.ToUnixNano(now)][1].Data))

	// Assert test data for direct writes is correct
	require.Equal(t, 1, len(directWritesSeriesMaps))

	require.Equal(t, 2, directWritesSeriesMaps[xtime.ToUnixNano(now)].Len())
	require.Equal(t, "foo", directWritesSeriesMaps[xtime.ToUnixNano(now)][0].ID.String())
	require.Equal(t, 120, len(directWritesSeriesMaps[xtime.ToUnixNano(now)][0].Data))
	require.Equal(t, "baz", directWritesSeriesMaps[xtime.ToUnixNano(now)][1].ID.String())
	require.Equal(t, 120, len(directWritesSeriesMaps[xtime.ToUnixNano(now)][1].Data))

	// Write data to first node
	err = writeTestDataToDisk(namesp, setups[0], firstNodeSeriesMaps, 0)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].StartServer())

	secondNodeIsUp := make(chan struct{})
	doneWriting := make(chan struct{})
	go func() {
		// Wait for bootstrapping to occur
		for reporter.Counters()["database.bootstrap.start"] == 0 {
			time.Sleep(10 * time.Millisecond)
		}

		<-secondNodeIsUp

		// Progress time before writing data directly to second node
		setups[1].SetNowFn(completeAt)

		// Write data that "arrives" at the second node directly
		err := setups[1].WriteBatch(namesp.ID(),
			directWritesSeriesMaps[xtime.ToUnixNano(now)])
		if err != nil {
			panic(err)
		}

		doneWriting <- struct{}{}
	}()

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].StartServer())
	log.Debug("servers are now up")

	secondNodeIsUp <- struct{}{}
	<-doneWriting

	// Stop the servers
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setups[0], namesp.ID(), firstNodeSeriesMaps)
	verifySeriesMaps(t, setups[1], namesp.ID(), seriesMaps)
}
