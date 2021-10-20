// Copyright (c) 2018 Uber Technologies, Inc.
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

package fs

import (
	"fmt"
	iofs "io/fs"
	"io/ioutil"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	xerror "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotIndexWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newIndexWriteTestSetup(t)
	defer test.cleanup()

	testSnapshotSegments := []struct {
		snapshotIndex int
		snapshotTime  xtime.UnixNano
		shards        map[uint32]struct{}
		segments      []testIndexSegment
	}{
		{
			snapshotIndex: 0,
			snapshotTime:  test.now.Add(-2 * time.Minute),
			shards:        shardsSet(1, 2, 4),
			segments: []testIndexSegment{
				{
					segmentType:  idxpersist.IndexSegmentType("fst"),
					majorVersion: 1,
					minorVersion: 2,
					metadata:     []byte("some_metadata"),
					files: []testIndexSegmentFile{
						{idxpersist.IndexSegmentFileType("first"), randDataFactorOfBuffSize(t, 1.5)},
						{idxpersist.IndexSegmentFileType("second"), randDataFactorOfBuffSize(t, 2.5)},
					},
				},
			},
		},
		{
			snapshotIndex: 1,
			snapshotTime:  test.now.Add(-1 * time.Minute),
			shards:        shardsSet(1, 3, 5),
			segments: []testIndexSegment{
				{
					segmentType:  idxpersist.IndexSegmentType("fst"),
					majorVersion: 3,
					minorVersion: 4,
					metadata:     []byte("some_other_metadata"),
					files: []testIndexSegmentFile{
						{idxpersist.IndexSegmentFileType("first"), randDataFactorOfBuffSize(t, 1.5)},
						{idxpersist.IndexSegmentFileType("second"), randDataFactorOfBuffSize(t, 3.5)},
					},
				},
			},
		},
	}

	// Write the data out
	writer := newTestIndexWriter(t, test.filePathPrefix)
	for _, snapshot := range testSnapshotSegments {
		// Add the snapshot index to the file set ID
		fileSetID := test.fileSetID
		fileSetID.VolumeIndex = snapshot.snapshotIndex

		err := writer.Open(IndexWriterOpenOptions{
			Identifier:  fileSetID,
			BlockSize:   test.blockSize,
			FileSetType: persist.FileSetSnapshotType,
			Snapshot: IndexWriterSnapshotOptions{
				SnapshotTime: snapshot.snapshotTime,
			},
			Shards: snapshot.shards,
		})
		require.NoError(t, err)

		writeTestIndexSegments(t, ctrl, writer, snapshot.segments)

		err = writer.Close()
		require.NoError(t, err)
	}

	// Verify files look good
	snapshotsDir := NamespaceIndexSnapshotDirPath(test.filePathPrefix,
		test.fileSetID.Namespace)
	files, err := ioutil.ReadDir(snapshotsDir)
	require.NoError(t, err)

	var actualFiles []string
	for _, file := range files {
		actualFiles = append(actualFiles, file.Name())
	}
	require.Equal(t, []string{
		fmt.Sprintf("fileset-%d-0-checkpoint.db", test.blockStart),
		fmt.Sprintf("fileset-%d-0-digest.db", test.blockStart),
		fmt.Sprintf("fileset-%d-0-info.db", test.blockStart),
		fmt.Sprintf("fileset-%d-0-segment-0-first.db", test.blockStart),
		fmt.Sprintf("fileset-%d-0-segment-0-second.db", test.blockStart),
		fmt.Sprintf("fileset-%d-1-checkpoint.db", test.blockStart),
		fmt.Sprintf("fileset-%d-1-digest.db", test.blockStart),
		fmt.Sprintf("fileset-%d-1-info.db", test.blockStart),
		fmt.Sprintf("fileset-%d-1-segment-0-first.db", test.blockStart),
		fmt.Sprintf("fileset-%d-1-segment-0-second.db", test.blockStart),
	}, actualFiles)

	// Verify can read them
	reader := newTestIndexReader(t, test.filePathPrefix,
		testIndexReaderOptions{})
	for _, snapshot := range testSnapshotSegments {
		// Add the snapshot index to the file set ID
		fileSetID := test.fileSetID
		fileSetID.VolumeIndex = snapshot.snapshotIndex

		result, err := reader.Open(IndexReaderOpenOptions{
			Identifier:  fileSetID,
			FileSetType: persist.FileSetSnapshotType,
		})
		require.NoError(t, err)
		require.Equal(t, snapshot.shards, result.Shards)

		readTestIndexSegments(t, ctrl, reader, snapshot.segments)

		err = reader.Validate()
		require.NoError(t, err)

		err = reader.Close()
		require.NoError(t, err)
	}
}

func TestIndexWriterFilesetErrExists(t *testing.T) {
	test := newIndexWriteTestSetup(t)
	defer test.cleanup()

	writer := newTestIndexWriter(t, test.filePathPrefix)

	indexWriterOpenOptions := IndexWriterOpenOptions{
		Identifier:  test.fileSetID,
		BlockSize:   test.blockSize,
		FileSetType: persist.FileSetSnapshotType,
		Shards:      map[uint32]struct{}{3: {}},
	}

	writeFn := func() error {
		if err := writer.Open(indexWriterOpenOptions); err != nil {
			return err
		}
		return writer.Close()
	}

	err := writeFn()
	require.NoError(t, err)

	err = writeFn()
	require.Error(t, err)
	assert.True(t, xerror.Is(err, iofs.ErrExist))
}
