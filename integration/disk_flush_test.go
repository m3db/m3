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

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/require"
)

var (
	errDiskFlushTimedOut = errors.New("flushing data to disk took too long")
)

func waitUntilDataFlushed(
	filePathPrefix string,
	shardSet sharding.ShardSet,
	namespace ts.ID,
	testData map[time.Time]seriesList,
	timeout time.Duration,
) error {
	dataFlushed := func() bool {
		for timestamp, seriesList := range testData {
			for _, series := range seriesList {
				shard := shardSet.Lookup(series.ID)
				if !fs.FilesetExistsAt(filePathPrefix, namespace, shard, timestamp) {
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
	reader fs.FileSetReader,
	shardSet sharding.ShardSet,
	iteratorPool encoding.ReaderIteratorPool,
	timestamp time.Time,
	namespace ts.ID,
	expected seriesList,
) {
	shards := make(map[uint32]struct{})
	for _, series := range expected {
		shard := shardSet.Lookup(series.ID)
		shards[shard] = struct{}{}
	}
	actual := make(seriesList, 0, len(expected))
	for shard := range shards {
		require.NoError(t, reader.Open(namespace, shard, timestamp))
		for i := 0; i < reader.Entries(); i++ {
			id, data, _, err := reader.Read()
			require.NoError(t, err)

			data.IncRef()

			var datapoints []ts.Datapoint
			it := iteratorPool.Get()
			it.Reset(bytes.NewBuffer(data.Get()))
			for it.Next() {
				dp, _, _ := it.Current()
				datapoints = append(datapoints, dp)
			}
			require.NoError(t, it.Err())
			it.Close()

			actual = append(actual, series{
				ID:   id,
				Data: datapoints,
			})

			data.DecRef()
			data.Finalize()
		}
		require.NoError(t, reader.Close())
	}

	compareSeriesList(t, expected, actual)
}

func verifyFlushed(
	t *testing.T,
	shardSet sharding.ShardSet,
	opts storage.Options,
	namespace ts.ID,
	seriesMaps map[time.Time]seriesList,
) {
	fsOpts := opts.CommitLogOptions().FilesystemOptions()
	reader := fs.NewReader(fsOpts.FilePathPrefix(), fsOpts.ReaderBufferSize(), opts.BytesPool(), nil)
	iteratorPool := opts.ReaderIteratorPool()
	for timestamp, seriesList := range seriesMaps {
		verifyForTime(t, reader, shardSet, iteratorPool, timestamp, namespace, seriesList)
	}
}

func TestDiskFlush(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	testSetup.storageOpts =
		testSetup.storageOpts.
			SetRetentionOptions(testSetup.storageOpts.RetentionOptions().
				SetBufferDrain(3 * time.Second).
				SetRetentionPeriod(6 * time.Hour))

	blockSize := testSetup.storageOpts.RetentionOptions().BlockSize()
	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Debug("disk flush test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.getNowFn()
	seriesMaps := make(map[time.Time]seriesList)
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
		seriesMaps[input.start] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.setNowFn(testSetup.getNowFn().Add(blockSize * 2))
	waitTimeout := testSetup.storageOpts.RetentionOptions().BufferDrain() * 10
	require.NoError(t, waitUntilDataFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[0], seriesMaps, waitTimeout))

	// Verify on-disk data match what we expect
	verifyFlushed(t, testSetup.shardSet, testSetup.storageOpts, testNamespaces[0], seriesMaps)
}
