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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestSnapshotMetadataWriteAndRead(t *testing.T) {
	var (
		dir            = createTempDir(t)
		filePathPrefix = filepath.Join(dir, "")
		opts           = testDefaultOpts.
				SetFilePathPrefix(filePathPrefix)
		commitlogIdentifier = persist.CommitlogFile{
			FilePath: "some_path",
			Start:    time.Now().Truncate(time.Second),
			Duration: 10 * time.Minute,
			Index:    1,
		}
		numMetadataFiles = 10
	)
	defer func() {
		os.RemoveAll(dir)
	}()

	var (
		reader = NewSnapshotMetadataReader(opts)
		writer = NewSnapshotMetadataWriter(opts)
	)
	for i := 0; i < numMetadataFiles; i++ {
		snapshotUUID := uuid.Parse("6645a373-bf82-42e7-84a6-f8452b137549")
		require.NotNil(t, snapshotUUID)

		var (
			snapshotMetadataIdentifier = SnapshotMetadataIdentifier{
				Index: int64(i),
				UUID:  snapshotUUID,
			}
		)

		err := writer.Write(SnapshotMetadataWriteArgs{
			ID:                  snapshotMetadataIdentifier,
			CommitlogIdentifier: commitlogIdentifier,
		})
		require.NoError(t, err)

		snapshotMetadata, err := reader.Read(snapshotMetadataIdentifier)
		require.NoError(t, err)

		require.Equal(t, SnapshotMetadata{
			ID:                  snapshotMetadataIdentifier,
			CommitlogIdentifier: commitlogIdentifier,
			MetadataFilePath: snapshotMetadataFilePathFromIdentifier(
				filePathPrefix, snapshotMetadataIdentifier),
			CheckpointFilePath: snapshotMetadataCheckpointFilePathFromIdentifier(
				filePathPrefix, snapshotMetadataIdentifier),
		}, snapshotMetadata)
	}
}
