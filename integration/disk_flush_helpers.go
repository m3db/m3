// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/require"
)

var (
	errDiskFlushTimedOut = errors.New("flushing data to disk took too long")
)

// nolint: deadcode
func waitUntilDataFlushed(
	filePathPrefix string,
	shardSet sharding.ShardSet,
	namespace ts.ID,
	testData map[time.Time]generate.SeriesBlock,
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
	expected generate.SeriesBlock,
) {
	shards := make(map[uint32]struct{})
	for _, series := range expected {
		shard := shardSet.Lookup(series.ID)
		shards[shard] = struct{}{}
	}
	actual := make(generate.SeriesBlock, 0, len(expected))
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

			actual = append(actual, generate.Series{
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

// nolint: deadcode
func verifyFlushed(
	t *testing.T,
	shardSet sharding.ShardSet,
	opts storage.Options,
	namespace ts.ID,
	seriesMaps map[time.Time]generate.SeriesBlock,
) {
	fsOpts := opts.CommitLogOptions().FilesystemOptions()
	reader := fs.NewReader(fsOpts.FilePathPrefix(), fsOpts.DataReaderBufferSize(), fsOpts.InfoReaderBufferSize(), opts.BytesPool(), nil)
	iteratorPool := opts.ReaderIteratorPool()
	for timestamp, seriesList := range seriesMaps {
		verifyForTime(t, reader, shardSet, iteratorPool, timestamp, namespace, seriesList)
	}
}
