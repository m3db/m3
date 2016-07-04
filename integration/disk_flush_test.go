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
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"

	"github.com/stretchr/testify/require"
)

var (
	errDiskFlushTimedOut = errors.New("flushing data to disk took too long")
)

func waitUntilDataFlushed(
	filePathPrefix string,
	shardingScheme m3db.ShardScheme,
	dataMaps map[time.Time]dataMap,
	timeout time.Duration,
) error {
	dataFlushed := func() bool {
		for timestamp, dm := range dataMaps {
			for id := range dm {
				shard := shardingScheme.Shard(id)
				if !fs.FileExistsAt(filePathPrefix, shard, timestamp) {
					return false
				}
			}
		}
		return true
	}
	if waitUntil(dataFlushed, timeout) {
		return nil
	}
	return errDiskFlushTimedOut
}

func verifyForTime(
	t *testing.T,
	reader m3db.FileSetReader,
	shardingScheme m3db.ShardScheme,
	decoder m3db.Decoder,
	timestamp time.Time,
	expected dataMap,
) {
	shards := make(map[uint32]struct{})
	for id := range expected {
		shard := shardingScheme.Shard(id)
		shards[shard] = struct{}{}
	}
	actual := make(dataMap, len(expected))
	for shard := range shards {
		require.NoError(t, reader.Open(shard, timestamp))
		for i := 0; i < reader.Entries(); i++ {
			id, data, err := reader.Read()
			require.NoError(t, err)

			var datapoints []m3db.Datapoint
			it := decoder.Decode(bytes.NewReader(data))
			for it.Next() {
				dp, _, _ := it.Current()
				datapoints = append(datapoints, dp)
			}
			require.NoError(t, it.Err())
			actual[id] = datapoints
		}
		require.NoError(t, reader.Close())
	}
	require.Equal(t, expected, actual)
}

func verifyFlushed(
	t *testing.T,
	shardingScheme m3db.ShardScheme,
	opts m3db.DatabaseOptions,
	dataMaps map[time.Time]dataMap,
) {
	readerFn := opts.GetNewFileSetReaderFn()
	reader := readerFn(opts.GetFilePathPrefix())
	decoderFn := opts.GetNewDecoderFn()
	decoder := decoderFn()
	for timestamp, dm := range dataMaps {
		verifyForTime(t, reader, shardingScheme, decoder, timestamp, dm)
	}
}

func TestDiskFlush(t *testing.T) {
	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	testSetup.dbOpts = testSetup.dbOpts.BufferDrain(3 * time.Second).RetentionPeriod(6 * time.Hour)
	blockSize := testSetup.dbOpts.GetBlockSize()
	filePathPrefix := testSetup.dbOpts.GetFilePathPrefix()

	// Start the server
	log := testSetup.dbOpts.GetLogger()
	log.Debug("disk flush test")
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
		{[]string{"foo", "baz"}, 50, now.Add(blockSize)},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.start)
		testData := generateTestData(input.metricNames, input.numPoints, input.start)
		dataMaps[input.start] = testData
		require.NoError(t, testSetup.writeBatch(testData))
	}
	log.Debug("test data is now written")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.setNowFn(testSetup.getNowFn().Add(blockSize * 2))
	waitTimeout := testSetup.dbOpts.GetBufferDrain() * 3
	require.NoError(t, waitUntilDataFlushed(filePathPrefix, testSetup.shardingScheme, dataMaps, waitTimeout))

	// Verify on-disk data match what we expect
	verifyFlushed(t, testSetup.shardingScheme, testSetup.dbOpts, dataMaps)
}
