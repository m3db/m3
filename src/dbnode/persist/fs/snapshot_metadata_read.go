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
	"encoding/json"
	"fmt"
	"os"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/proto/snapshot"
	"github.com/m3db/m3/src/dbnode/persist"

	"github.com/pborman/uuid"
)

// NewSnapshotMetadataReader returns a new SnapshotMetadataReader.
func NewSnapshotMetadataReader(opts Options) *SnapshotMetadataReader {
	return &SnapshotMetadataReader{
		opts: opts,

		metadataReader: digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
	}
}

// SnapshotMetadataReader is a reader for SnapshotMetadata.
type SnapshotMetadataReader struct {
	opts Options

	metadataReader digest.FdWithDigestReader
}

func (w *SnapshotMetadataReader) Read(id SnapshotMetadataIdentifier) (SnapshotMetadata, error) {
	var (
		prefix         = w.opts.FilePathPrefix()
		checkpointPath = snapshotMetadataCheckpointFilePathFromIdentifier(prefix, id)
		metadataPath   = snapshotMetadataFilePathFromIdentifier(prefix, id)
	)

	expectedDigest, err := readCheckpointFile(checkpointPath, digest.NewBuffer())
	if err != nil {
		return SnapshotMetadata{}, err
	}

	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	w.metadataReader.Reset(metadataFile)

	metadataFileInfo, err := metadataFile.Stat()
	if err != nil {
		return SnapshotMetadata{}, err
	}

	var (
		size = metadataFileInfo.Size()
		buf  = make([]byte, int(size))
	)
	n, err := w.metadataReader.ReadAllAndValidate(buf, expectedDigest)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	if int64(n) != size {
		// Should never happen
		return SnapshotMetadata{}, fmt.Errorf("read: %d bytes of metadata file, but expected: %d",
			n, size)
	}

	protoMetadata := &snapshot.Metadata{}
	err = protoMetadata.Unmarshal(buf[:n])
	if err != nil {
		return SnapshotMetadata{}, err
	}

	parsedUUID, err := uuid.ParseBytes(protoMetadata.SnapshotUUID)
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("unable to parse UUID: %v, err: %v", protoMetadata.SnapshotUUID, err)
	}

	commitlogID := &persist.CommitlogFile{}
	err = json.Unmarshal(protoMetadata.CommitlogID, commitlogID)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	return SnapshotMetadata{
		ID: SnapshotMetadataIdentifier{
			Index: protoMetadata.SnapshotIndex,
			UUID:  parsedUUID,
		},
		CommitlogIdentifier: *commitlogID,
		MetadataFilePath:    snapshotMetadataFilePathFromIdentifier(prefix, id),
		CheckpointFilePath:  snapshotMetadataCheckpointFilePathFromIdentifier(prefix, id),
	}, nil
}
