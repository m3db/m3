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

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestTruncateNamespace(t *testing.T) {
	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	testSetup.storageOpts =
		testSetup.storageOpts.RetentionOptions(testSetup.storageOpts.GetRetentionOptions().
			BufferDrain(time.Second).
			RetentionPeriod(6 * time.Hour))
	blockSize := testSetup.storageOpts.GetRetentionOptions().GetBlockSize()

	// Start the server
	log := testSetup.storageOpts.GetInstrumentOptions().GetLogger()
	log.Debug("truncate namespace test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.getNowFn()
	dataMaps := make(map[time.Time]seriesList)
	inputData := []struct {
		namespace string
		ids       []string
		numPoints int
		start     time.Time
	}{
		{testNamespaces[0], []string{"foo"}, 100, now},
		{testNamespaces[1], []string{"bar"}, 50, now.Add(blockSize)},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.start)
		testData := generateTestData(input.namespace, input.ids, input.numPoints, input.start)
		dataMaps[input.start] = testData
		require.NoError(t, testSetup.writeBatch(testData))
	}
	log.Debug("test data is now written")

	fetchReq := rpc.NewFetchRequest()
	fetchReq.IdWithNamespace = rpc.NewIDWithNamespace()
	fetchReq.IdWithNamespace.ID = "foo"
	fetchReq.IdWithNamespace.Ns = testNamespaces[1]
	fetchReq.RangeStart = xtime.ToNormalizedTime(now, time.Second)
	fetchReq.RangeEnd = xtime.ToNormalizedTime(now.Add(blockSize), time.Second)
	fetchReq.ResultTimeType = rpc.TimeType_UNIX_SECONDS

	log.Debug("fetching data from nonexistent namespace")
	fetchReq.IdWithNamespace.Ns = "nonexistent"
	_, err = testSetup.fetch(fetchReq)
	require.Error(t, err)

	log.Debug("fetching data from wrong namespace")
	fetchReq.IdWithNamespace.Ns = testNamespaces[1]
	res, err := testSetup.fetch(fetchReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))

	log.Debugf("fetching data from namespace %s", testNamespaces[0])
	fetchReq.IdWithNamespace.Ns = testNamespaces[0]
	res, err = testSetup.fetch(fetchReq)
	require.NoError(t, err)
	require.Equal(t, 100, len(res))

	log.Debugf("truncate namespace %s", testNamespaces[0])
	truncateReq := rpc.NewTruncateNamespaceRequest()
	truncateReq.Ns = testNamespaces[0]
	err = testSetup.truncate(truncateReq)
	require.NoError(t, err)

	log.Debugf("fetching data from namespace %s again", testNamespaces[0])
	res, err = testSetup.fetch(fetchReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))

	log.Debugf("fetching data from a different namespace %s", testNamespaces[1])
	fetchReq.IdWithNamespace.ID = "bar"
	fetchReq.IdWithNamespace.Ns = testNamespaces[1]
	fetchReq.RangeStart = xtime.ToNormalizedTime(now.Add(blockSize), time.Second)
	fetchReq.RangeEnd = xtime.ToNormalizedTime(now.Add(blockSize*2), time.Second)
	res, err = testSetup.fetch(fetchReq)
	require.NoError(t, err)
	require.Equal(t, 50, len(res))
}
