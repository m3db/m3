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

	"github.com/stretchr/testify/require"
)

func TestRoundtrip(t *testing.T) {
	// Test setup
	testSetup, err := newTestSetup(newOptions())
	require.NoError(t, err)
	defer testSetup.close()
	// Start the server
	log := testSetup.dbOpts.GetLogger()
	log.Debug("round trip test")
	doneCh := make(chan struct{})
	require.NoError(t, testSetup.startServer(doneCh))
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer(doneCh))
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.getNowFn()
	dataMaps := make(map[time.Time]dataMap)
	inputData := []struct {
		metricNames []string
		numPoints   int
		start       time.Time
	}{
		{[]string{"foo", "bar"}, 100, now},
		{[]string{"foo", "baz"}, 50, now.Add(testSetup.dbOpts.GetBlockSize())},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.start)
		testData := generateTestData(input.metricNames, input.numPoints, input.start)
		dataMaps[input.start] = testData
		require.NoError(t, testSetup.writeBatch(testData))
	}
	log.Debug("test data is now written")

	// Verify in-memory data match what we've written
	verifyDataMaps(t, testSetup, dataMaps)
}
