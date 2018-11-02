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
	"os"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/proto/snapshot"
)

type snapshotMetadataWriter struct {
	opts Options
}

type snapshotMetadataWriteArgs struct {
	ID SnapshotMetadataIdentifier
	// TODO: Fix me
	CommitlogIdentifier []byte
}

func (w *snapshotMetadataWriter) Write(args snapshotMetadataWriteArgs) error {
	var (
		prefix       = w.opts.FilePathPrefix()
		snapshotsDir = SnapshotsDirPath(prefix)
	)
	if err := os.MkdirAll(snapshotsDir, w.opts.NewDirectoryMode()); err != nil {
		return err
	}
	snapshotsDirFile, err := os.Open(snapshotsDir)
	if err != nil {
		return err
	}
	defer snapshotsDirFile.Close()

	metadataPath := snapshotMetadataFilePathFromIdentifier(prefix, args.ID)
	metadataFile, err := OpenWritable(metadataPath, w.opts.NewFileMode())
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	metadataFdWithDigest := digest.NewFdWithDigestWriter(w.opts.WriterBufferSize())
	metadataFdWithDigest.Reset(metadataFile)

	metadataBytes, err := (&snapshot.Metadata{
		SnapshotIndex:       args.ID.Index,
		SnapshotID:          []byte(args.ID.ID.String()),
		CommitlogIdentifier: args.CommitlogIdentifier,
	}).Marshal()
	if err != nil {
		return err
	}

	written, err := metadataFdWithDigest.Write(metadataBytes)
	if err != nil {
		return err
	}
	if written != len(metadataBytes) {
		// Should never happen
		return fmt.Errorf("did not complete writing metadata bytes, should have written: %d but wrote: %d bytes",
			written, len(metadataBytes))
	}

	// Sync the file and its parent to ensure they're actually written out.
	err = metadataFile.Sync()
	if err != nil {
		return err
	}
	err = snapshotsDirFile.Sync()
	if err != nil {
		return err
	}

	checkpointPath := snapshotMetadataCheckpointFilePathFromIdentifier(prefix, args.ID)
	checkpointFile, err := OpenWritable(checkpointPath, w.opts.NewFileMode())
	if err != nil {
		return err
	}
	defer checkpointFile.Close()

	checkpointFdWithDigest := digest.NewFdWithDigestContentsWriter(w.opts.WriterBufferSize())
	checkpointFdWithDigest.Reset(metadataFile)

	err = checkpointFdWithDigest.WriteDigests(metadataFdWithDigest.Digest().Sum32())
	if err != nil {
		return err
	}

	// Sync the file and its parent to ensure they're actually written out.
	err = checkpointFile.Sync()
	if err != nil {
		return err
	}
	err = snapshotsDirFile.Sync()
	if err != nil {
		return err
	}

	return nil
}
