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
	xerrors "github.com/m3db/m3x/errors"

	"github.com/gogo/protobuf/proto"
)

type cleanupFn func() error

// NewSnapshotMetadataWriter constructs a new snapshot metadata writer.
func NewSnapshotMetadataWriter(opts Options) *SnapshotMetadataWriter {
	return &SnapshotMetadataWriter{
		opts: opts,

		metadataFdWithDigest: digest.NewFdWithDigestWriter(opts.WriterBufferSize()),
		digestBuf:            digest.NewBuffer(),
	}
}

// SnapshotMetadataWriter is a writer for SnapshotMetadata.
type SnapshotMetadataWriter struct {
	opts Options

	metadataFdWithDigest digest.FdWithDigestWriter
	digestBuf            digest.Buffer
}

// SnapshotMetadataWriteArgs are the arguments for SnapshotMetadataWriter.Write.
type SnapshotMetadataWriteArgs struct {
	ID                  SnapshotMetadataIdentifier
	CommitlogIdentifier persist.CommitlogFile
}

func (w *SnapshotMetadataWriter) Write(args SnapshotMetadataWriteArgs) (finalErr error) {
	var (
		prefix       = w.opts.FilePathPrefix()
		snapshotsDir = SnapshotsDirPath(prefix)

		// Set this up so that we can defer cleanup functions without ignoring
		// the errors.
		cleanupFns   = []cleanupFn{}
		deferCleanup = func(f cleanupFn) {
			cleanupFns = append(cleanupFns, f)
		}
	)
	defer func() {
		multiErr := xerrors.MultiError{}
		if finalErr != nil {
			multiErr = multiErr.Add(finalErr)
		}

		for _, f := range cleanupFns {
			err := f()
			if err != nil {
				multiErr = multiErr.Add(err)
			}
		}

		finalErr = multiErr.FinalError()
	}()

	if err := os.MkdirAll(snapshotsDir, w.opts.NewDirectoryMode()); err != nil {
		return err
	}
	snapshotsDirFile, err := os.Open(snapshotsDir)
	if err != nil {
		return err
	}
	deferCleanup(snapshotsDirFile.Close)

	metadataPath := snapshotMetadataFilePathFromIdentifier(prefix, args.ID)
	metadataFile, err := OpenWritable(metadataPath, w.opts.NewFileMode())
	if err != nil {
		return err
	}

	w.metadataFdWithDigest.Reset(metadataFile)
	deferCleanup(w.metadataFdWithDigest.Close)

	// TODO(rartoul): Fix this
	commitlogIDBytes, err := json.Marshal(args.CommitlogIdentifier)
	if err != nil {
		return err
	}
	metadataBytes, err := proto.Marshal(&snapshot.Metadata{
		SnapshotIndex: args.ID.Index,
		SnapshotUUID:  []byte(args.ID.UUID.String()),
		CommitlogID:   commitlogIDBytes,
	})
	if err != nil {
		return err
	}

	written, err := w.metadataFdWithDigest.Write(metadataBytes)
	if err != nil {
		return err
	}
	if written != len(metadataBytes) {
		// Should never happen
		return fmt.Errorf("did not complete writing metadata bytes, should have written: %d but wrote: %d bytes",
			written, len(metadataBytes))
	}

	err = w.metadataFdWithDigest.Flush()
	if err != nil {
		return err
	}

	// Ensure the file is written out.
	err = w.sync(metadataFile, snapshotsDirFile)
	if err != nil {
		return err
	}

	checkpointPath := snapshotMetadataCheckpointFilePathFromIdentifier(prefix, args.ID)
	checkpointFile, err := OpenWritable(checkpointPath, w.opts.NewFileMode())
	if err != nil {
		return err
	}
	deferCleanup(checkpointFile.Close)

	// digestBuf does not need to be reset.
	err = w.digestBuf.WriteDigestToFile(
		checkpointFile, w.metadataFdWithDigest.Digest().Sum32())
	if err != nil {
		return err
	}

	// Ensure the file is written out.
	err = w.sync(checkpointFile, snapshotsDirFile)
	if err != nil {
		return err
	}

	return nil
}

// sync ensures that the provided file is persisted to disk by syncing it, as well
// as its parent directory (ensuring the file is discoverable in the parent inode.)
func (w *SnapshotMetadataWriter) sync(file, parent *os.File) error {
	err := file.Sync()
	if err != nil {
		return err
	}

	err = parent.Sync()
	if err != nil {
		return err
	}

	return nil
}
