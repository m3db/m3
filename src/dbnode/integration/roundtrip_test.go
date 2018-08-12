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
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestRoundtrip(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := newTestOptions(t).
		SetTickMinimumInterval(time.Second)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	blockSize := testOpts.CommitLogBlockSize()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Debug("round trip test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.getNowFn()
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(blockSize)},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time and sleep for a long enough time so data blocks are sealed during ticking
	testSetup.setNowFn(testSetup.getNowFn().Add(blockSize * 2))
	testSetup.sleepFor10xTickMinimumInterval()

	// Verify in-memory data match what we've written
	verifySeriesMaps(t, testSetup, testNamespaces[0], seriesMaps)

	// Verify in-memory data again just to be sure the data can be read multiple times without issues
	verifySeriesMaps(t, testSetup, testNamespaces[0], seriesMaps)
}
