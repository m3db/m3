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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/ident/testutil"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	errDiskFlushTimedOut = errors.New("flushing data to disk took too long")
)

type snapshotID struct {
	blockStart time.Time
	minVolume  int
}

func getLatestSnapshotVolumeIndex(
	filePathPrefix string,
	shardSet sharding.ShardSet,
	namespace ident.ID,
	blockStart time.Time,
) int {
	latestVolumeIndex := -1

	for _, shard := range shardSet.AllIDs() {
		snapshotFiles, err := fs.SnapshotFiles(
			filePathPrefix, namespace, shard)
		if err != nil {
			panic(err)
		}
		latestSnapshot, ok := snapshotFiles.LatestVolumeForBlock(blockStart)
		if !ok {
			continue
		}
		if latestSnapshot.ID.VolumeIndex > latestVolumeIndex {
			latestVolumeIndex = latestSnapshot.ID.VolumeIndex
		}
	}

	return latestVolumeIndex
}

func waitUntilSnapshotFilesFlushed(
	filePathPrefix string,
	shardSet sharding.ShardSet,
	namespace ident.ID,
	expectedSnapshots []snapshotID,
	timeout time.Duration,
) error {
	dataFlushed := func() bool {
		for _, shard := range shardSet.AllIDs() {
			for _, e := range expectedSnapshots {
				snapshotFiles, err := fs.SnapshotFiles(
					filePathPrefix, namespace, shard)
				if err != nil {
					panic(err)
				}

				latest, ok := snapshotFiles.LatestVolumeForBlock(e.blockStart)
				if !ok {
					return false
				}

				if !(latest.ID.VolumeIndex >= e.minVolume) {
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

func waitUntilDataFilesFlushed(
	filePathPrefix string,
	shardSet sharding.ShardSet,
	namespace ident.ID,
	testData map[xtime.UnixNano]generate.SeriesBlock,
	timeout time.Duration,
) error {
	dataFlushed := func() bool {
		for timestamp, seriesList := range testData {
			for _, series := range seriesList {
				shard := shardSet.Lookup(series.ID)
				exists, err := fs.DataFileSetExistsAt(
					filePathPrefix, namespace, shard, timestamp.ToTime())
				if err != nil {
					panic(err)
				}

				if !exists {
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
	storageOpts storage.Options,
	reader fs.DataFileSetReader,
	shardSet sharding.ShardSet,
	iteratorPool encoding.ReaderIteratorPool,
	timestamp time.Time,
	namespace ident.ID,
	filesetType persist.FileSetType,
	expected generate.SeriesBlock,
) {
	shards := make(map[uint32]struct{})
	for _, series := range expected {
		shard := shardSet.Lookup(series.ID)
		shards[shard] = struct{}{}
	}
	actual := make(generate.SeriesBlock, 0, len(expected))
	for shard := range shards {
		rOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: timestamp,
			},
			FileSetType: filesetType,
		}

		if filesetType == persist.FileSetSnapshotType {
			// If we're verifying snapshot files, then we need to identify the latest
			// one because multiple snapshot files can exist at the same time with the
			// same blockStart, but increasing "indexes" which indicates which one is
			// most recent (and thus has more cumulative data).
			filePathPrefix := storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
			snapshotFiles, err := fs.SnapshotFiles(filePathPrefix, namespace, shard)
			require.NoError(t, err)
			latest, ok := snapshotFiles.LatestVolumeForBlock(timestamp)
			require.True(t, ok)
			rOpts.Identifier.VolumeIndex = latest.ID.VolumeIndex
		}
		require.NoError(t, reader.Open(rOpts))
		for i := 0; i < reader.Entries(); i++ {
			id, tagsIter, data, _, err := reader.Read()
			require.NoError(t, err)

			tags, err := testutil.NewTagsFromTagIterator(tagsIter)
			require.NoError(t, err)

			data.IncRef()

			var datapoints []ts.Datapoint
			it := iteratorPool.Get()
			it.Reset(bytes.NewBuffer(data.Bytes()))
			for it.Next() {
				dp, _, _ := it.Current()
				datapoints = append(datapoints, dp)
			}
			require.NoError(t, it.Err())
			it.Close()

			actual = append(actual, generate.Series{
				ID:   id,
				Tags: tags,
				Data: datapoints,
			})

			data.DecRef()
			data.Finalize()
		}
		require.NoError(t, reader.Close())
	}

	compareSeriesList(t, expected, actual)
}

func verifyFlushedDataFiles(
	t *testing.T,
	shardSet sharding.ShardSet,
	storageOpts storage.Options,
	namespace ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	reader, err := fs.NewReader(storageOpts.BytesPool(), fsOpts)
	require.NoError(t, err)
	iteratorPool := storageOpts.ReaderIteratorPool()
	for timestamp, seriesList := range seriesMaps {
		verifyForTime(
			t, storageOpts, reader, shardSet, iteratorPool, timestamp.ToTime(),
			namespace, persist.FileSetFlushType, seriesList)
	}
}

func verifySnapshottedDataFiles(
	t *testing.T,
	shardSet sharding.ShardSet,
	storageOpts storage.Options,
	namespace ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	reader, err := fs.NewReader(storageOpts.BytesPool(), fsOpts)
	require.NoError(t, err)
	iteratorPool := storageOpts.ReaderIteratorPool()
	for blockStart, seriesList := range seriesMaps {

		verifyForTime(
			t, storageOpts, reader, shardSet, iteratorPool, blockStart.ToTime(),
			namespace, persist.FileSetSnapshotType, seriesList)
	}

}
