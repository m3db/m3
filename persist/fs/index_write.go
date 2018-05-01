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
	"io/ioutil"
	"os"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/index"
	"github.com/m3db/m3db/persist"
)

const (
	indexFileSetMajorVersion = 1
)

type indexWriter struct {
	opts             Options
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode

	err          error
	blockSize    time.Duration
	start        time.Time
	snapshotTime time.Time
	fileType     persist.FileSetType
	segments     []writtenIndexSegment

	checkpointFilePath string
	infoFilePath       string
	digestFilePath     string
}

type writtenIndexSegment struct {
	segmentType  IndexSegmentType
	majorVersion int
	minorVersion int
	metadata     []byte
	files        []writtenIndexSegmentFile
}

type writtenIndexSegmentFile struct {
	segmentFileType IndexSegmentFileType
	digest          uint32
}

// NewIndexWriter returns a new index writer with options.
func NewIndexWriter(opts Options) (IndexFileSetWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &indexWriter{
		opts:             opts,
		filePathPrefix:   opts.FilePathPrefix(),
		newFileMode:      opts.NewFileMode(),
		newDirectoryMode: opts.NewDirectoryMode(),
	}, nil
}

func (w *indexWriter) Open(opts IndexWriterOpenOptions) error {
	var (
		nextSnapshotIndex int
		err               error
		namespaceDir      string
		namespace         = opts.Identifier.Namespace
		blockStart        = opts.Identifier.BlockStart
	)
	w.err = nil
	w.blockSize = opts.BlockSize
	w.start = blockStart
	w.snapshotTime = opts.Snapshot.SnapshotTime
	w.segments = nil

	switch opts.FilesetType {
	case persist.FileSetSnapshotType:
		namespaceDir = NamespaceIndexSnapshotDirPath(w.filePathPrefix, namespace)
		// Can't do this outside of the switch statement because we need to make sure
		// the directory exists before calling NextSnapshotFileIndex
		if err := os.MkdirAll(namespaceDir, w.newDirectoryMode); err != nil {
			return err
		}

		// This method is not thread-safe, so its the callers responsibilities that they never
		// try and write two snapshot files for the same block start at the same time.
		nextSnapshotIndex, err = NextIndexSnapshotFileIndex(w.filePathPrefix, namespace, blockStart)
		if err != nil {
			return err
		}

		w.fileType = persist.FileSetSnapshotType
		w.checkpointFilePath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, checkpointFileSuffix, nextSnapshotIndex)
		w.infoFilePath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, infoFileSuffix, nextSnapshotIndex)
		w.digestFilePath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, digestFileSuffix, nextSnapshotIndex)
	case persist.FileSetFlushType:
		namespaceDir = NamespaceIndexDataDirPath(w.filePathPrefix, namespace)
		if err := os.MkdirAll(namespaceDir, w.newDirectoryMode); err != nil {
			return err
		}

		w.fileType = persist.FileSetFlushType
		w.checkpointFilePath = filesetPathFromTime(namespaceDir, blockStart, checkpointFileSuffix)
		w.infoFilePath = filesetPathFromTime(namespaceDir, blockStart, infoFileSuffix)
		w.digestFilePath = filesetPathFromTime(namespaceDir, blockStart, digestFileSuffix)
	}
	return nil
}

func (w *indexWriter) WriteSegmentFileSet(segmentFileSet IndexSegmentFileSet) error {
	if w.err != nil {
		return w.err
	}

	return nil
}

func (w *indexWriter) infoFileData() ([]byte, error) {
	info := &index.IndexInfo{
		MajorVersion: indexFileSetMajorVersion,
		BlockStart:   w.start.UnixNano(),
		BlockSize:    int64(w.blockSize),
		FileType:     int64(w.fileType),
		SnapshotTime: w.snapshotTime.UnixNano(),
	}
	return info.Marshal()
}

func (w *indexWriter) digestsFileData(infoFileData []byte) ([]byte, error) {
	digests := &index.IndexDigests{
		InfoDigest: digest.Checksum(infoFileData),
	}
	for _, segment := range w.segments {
		segmentDigest := &index.IndexDigests_SegmentDigest{
			Type: string(segment.segmentType),
		}
		for _, file := range segment.files {
			fileDigest := &index.IndexDigests_SegmentFileDigest{
				FileType: string(file.segmentFileType),
				Digest:   file.digest,
			}
			segmentDigest.Files = append(segmentDigest.Files, fileDigest)
		}
		digests.SegmentDigests = append(digests.SegmentDigests, segmentDigest)
	}
	return digests.Marshal()
}

func (w *indexWriter) Close() error {
	if w.err != nil {
		// If a write error occurred don't even bother trying to write out file set
		return w.err
	}

	// Write info file
	infoFileData, err := w.infoFileData()
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(w.infoFilePath, infoFileData, w.newFileMode)
	if err != nil {
		return err
	}

	// Write digests file
	digestsFileData, err := w.digestsFileData(infoFileData)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(w.digestFilePath, digestsFileData, w.newFileMode)
	if err != nil {
		return err
	}

	// Write checkpoint file
	fd, err := OpenWritable(w.checkpointFilePath, w.newFileMode)
	if err != nil {
		return err
	}

	bufferSize := w.opts.WriterBufferSize()
	digestBuffer := digest.NewFdWithDigestContentsWriter(bufferSize)
	digestBuffer.Reset(fd)
	err = digestBuffer.WriteDigests(digest.Checksum(digestsFileData))
	if err != nil {
		return err
	}
	if err := digestBuffer.Close(); err != nil {
		// NB(r): intentionally skipping fd.Close() error, as failure
		// to write takes precedence over failure to close the file
		fd.Close()
		return err
	}
	return fd.Close()
}
