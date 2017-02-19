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

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	xmetrics "github.com/m3db/m3db/x/metrics"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapMergeLocal(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setups
	log := xlog.SimpleLogger
	namesp := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
	opts := newTestOptions().
		SetNamespaces([]namespace.Metadata{namesp}).SetVerifySeriesDebugFilePathPrefix("/tmp/")

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())

	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute).
		SetBufferDrain(3 * time.Second)
	setupOpts := []bootstrappableTestSetupOptions{
		{disablePeersBootstrapper: true},
		{disablePeersBootstrapper: false, testStatsReporter: reporter},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, retentionOpts, setupOpts)
	defer closeFn()

	// Write test data for first node, ensure to overflow past
	now := setups[0].getNowFn()
	cutoverAt := now.Add(retentionOpts.BufferFuture())
	completeAt := now.Add(180 * time.Second)
	blockSize := setups[0].storageOpts.RetentionOptions().BlockSize()
	seriesMaps := generateTestDataByStart([]testData{
		{ids: []string{"foo", "bar"}, numPoints: 180, start: now.Add(-blockSize)},
		{ids: []string{"foo", "baz"}, numPoints: int(completeAt.Sub(now) / time.Second), start: now},
	})
	firstNodeSeriesMaps := map[time.Time]seriesList{}
	directWritesSeriesMaps := map[time.Time]seriesList{}
	for start, s := range seriesMaps {
		for i := range s {
			isPartialSeries := start.Equal(now)
			if !isPartialSeries {
				// Normal series should just be straight up copied from first node
				firstNodeSeriesMaps[start] = append(firstNodeSeriesMaps[start], s[i])
				continue
			}

			firstNodeSeries := series{ID: s[i].ID}
			directWritesSeries := series{ID: s[i].ID}
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

	require.Equal(t, 2, firstNodeSeriesMaps[now.Add(-blockSize)].Len())
	require.Equal(t, "foo", firstNodeSeriesMaps[now.Add(-blockSize)][0].ID.String())
	require.Equal(t, 180, len(firstNodeSeriesMaps[now.Add(-blockSize)][0].Data))
	require.Equal(t, "bar", firstNodeSeriesMaps[now.Add(-blockSize)][1].ID.String())
	require.Equal(t, 180, len(firstNodeSeriesMaps[now.Add(-blockSize)][1].Data))

	require.Equal(t, 2, firstNodeSeriesMaps[now].Len())
	require.Equal(t, "foo", firstNodeSeriesMaps[now][0].ID.String())
	require.Equal(t, 60, len(firstNodeSeriesMaps[now][0].Data))
	require.Equal(t, "baz", firstNodeSeriesMaps[now][1].ID.String())
	require.Equal(t, 60, len(firstNodeSeriesMaps[now][1].Data))

	// Assert test data for direct writes is correct
	require.Equal(t, 1, len(directWritesSeriesMaps))

	require.Equal(t, 2, directWritesSeriesMaps[now].Len())
	require.Equal(t, "foo", directWritesSeriesMaps[now][0].ID.String())
	require.Equal(t, 120, len(directWritesSeriesMaps[now][0].Data))
	require.Equal(t, "baz", directWritesSeriesMaps[now][1].ID.String())
	require.Equal(t, 120, len(directWritesSeriesMaps[now][1].Data))

	// Write data to first node
	err := writeTestDataToDisk(t, namesp.ID(), setups[0], firstNodeSeriesMaps)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	go func() {
		// Wait for bootstrapping to occur
		for reporter.Counters()["database.bootstrap.start"] == 0 {
			time.Sleep(10 * time.Millisecond)
		}

		// Progress time before writing data directly to second node
		setups[1].setNowFn(completeAt)

		// Write data that "arrives" at the second node directly
		require.NoError(t, setups[1].writeBatch(namesp.ID(), directWritesSeriesMaps[now]))
	}()

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
	verifySeriesMaps(t, setups[0], namesp.ID(), firstNodeSeriesMaps)
	verifySeriesMaps(t, setups[1], namesp.ID(), seriesMaps)
}
