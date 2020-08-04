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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	ns "github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/ident/testutil"
	xtime "github.com/m3db/m3/src/x/time"

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
				exists, err := fs.DataFileSetExists(
					filePathPrefix, namespace, shard, timestamp.ToTime(), 0)
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

func waitUntilFileSetFilesExist(
	filePathPrefix string,
	files []fs.FileSetFileIdentifier,
	timeout time.Duration,
) error {
	return waitUntilFileSetFilesExistOrNot(filePathPrefix, files, true, timeout)
}

func waitUntilFileSetFilesNotExist(
	filePathPrefix string,
	files []fs.FileSetFileIdentifier,
	timeout time.Duration,
) error {
	return waitUntilFileSetFilesExistOrNot(filePathPrefix, files, false, timeout)
}

func waitUntilFileSetFilesExistOrNot(
	filePathPrefix string,
	files []fs.FileSetFileIdentifier,
	// Either wait for all files to exist of for all files to not exist.
	checkForExistence bool,
	timeout time.Duration,
) error {
	dataFlushed := func() bool {
		for _, file := range files {
			exists, err := fs.DataFileSetExists(
				filePathPrefix, file.Namespace, file.Shard, file.BlockStart, file.VolumeIndex)
			if err != nil {
				panic(err)
			}

			if checkForExistence && !exists {
				return false
			}

			if !checkForExistence && exists {
				return false
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
	nsCtx ns.Context,
	filesetType persist.FileSetType,
	expected generate.SeriesBlock,
) {
	err := checkForTime(
		storageOpts, reader, shardSet, iteratorPool, timestamp,
		nsCtx, filesetType, expected)
	require.NoError(t, err)
}

func checkForTime(
	storageOpts storage.Options,
	reader fs.DataFileSetReader,
	shardSet sharding.ShardSet,
	iteratorPool encoding.ReaderIteratorPool,
	timestamp time.Time,
	nsCtx ns.Context,
	filesetType persist.FileSetType,
	expected generate.SeriesBlock,
) error {
	shards := make(map[uint32]struct{})
	for _, series := range expected {
		shard := shardSet.Lookup(series.ID)
		shards[shard] = struct{}{}
	}
	actual := make(generate.SeriesBlock, 0, len(expected))
	for shard := range shards {
		rOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  nsCtx.ID,
				Shard:      shard,
				BlockStart: timestamp,
			},
			FileSetType: filesetType,
		}

		filePathPrefix := storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
		switch filesetType {
		// Identify the latest volume for this block start.
		case persist.FileSetSnapshotType:
			snapshotFiles, err := fs.SnapshotFiles(filePathPrefix, nsCtx.ID, shard)
			if err != nil {
				return err
			}
			latest, ok := snapshotFiles.LatestVolumeForBlock(timestamp)
			if !ok {
				return fmt.Errorf("no latest snapshot volume for block: %v", timestamp)
			}
			rOpts.Identifier.VolumeIndex = latest.ID.VolumeIndex
		case persist.FileSetFlushType:
			dataFiles, err := fs.DataFiles(filePathPrefix, nsCtx.ID, shard)
			if err != nil {
				return err
			}
			latest, ok := dataFiles.LatestVolumeForBlock(timestamp)
			if !ok {
				return fmt.Errorf("no latest data volume for block: %v", timestamp)
			}
			rOpts.Identifier.VolumeIndex = latest.ID.VolumeIndex
		}
		if err := reader.Open(rOpts); err != nil {
			return err
		}

		for i := 0; i < reader.Entries(); i++ {
			id, tagsIter, data, _, err := reader.Read()
			if err != nil {
				return err
			}

			tags, err := testutil.NewTagsFromTagIterator(tagsIter)
			if err != nil {
				return err
			}

			data.IncRef()

			var datapoints []generate.TestValue
			it := iteratorPool.Get()
			it.Reset(bytes.NewBuffer(data.Bytes()), nsCtx.Schema)
			for it.Next() {
				dp, _, ann := it.Current()
				datapoints = append(datapoints, generate.TestValue{Datapoint: dp, Annotation: ann})
			}
			if err := it.Err(); err != nil {
				return err
			}
			it.Close()

			actual = append(actual, generate.Series{
				ID:   id,
				Tags: tags,
				Data: datapoints,
			})

			data.DecRef()
			data.Finalize()
		}
		if err := reader.Close(); err != nil {
			return err
		}
	}

	return compareSeriesList(expected, actual)
}

func verifyFlushedDataFiles(
	t *testing.T,
	shardSet sharding.ShardSet,
	storageOpts storage.Options,
	nsID ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) {
	err := checkFlushedDataFiles(shardSet, storageOpts, nsID, seriesMaps)
	require.NoError(t, err)
}

func checkFlushedDataFiles(
	shardSet sharding.ShardSet,
	storageOpts storage.Options,
	nsID ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) error {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	reader, err := fs.NewReader(storageOpts.BytesPool(), fsOpts)
	if err != nil {
		return err
	}
	iteratorPool := storageOpts.ReaderIteratorPool()
	nsCtx := ns.NewContextFor(nsID, storageOpts.SchemaRegistry())
	for timestamp, seriesList := range seriesMaps {
		err := checkForTime(
			storageOpts, reader, shardSet, iteratorPool, timestamp.ToTime(),
			nsCtx, persist.FileSetFlushType, seriesList)
		if err != nil {
			return err
		}
	}

	return nil
}

func verifySnapshottedDataFiles(
	t *testing.T,
	shardSet sharding.ShardSet,
	storageOpts storage.Options,
	nsID ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	reader, err := fs.NewReader(storageOpts.BytesPool(), fsOpts)
	require.NoError(t, err)
	iteratorPool := storageOpts.ReaderIteratorPool()
	nsCtx := ns.NewContextFor(nsID, storageOpts.SchemaRegistry())
	for blockStart, seriesList := range seriesMaps {
		verifyForTime(
			t, storageOpts, reader, shardSet, iteratorPool, blockStart.ToTime(),
			nsCtx, persist.FileSetSnapshotType, seriesList)
	}

}
