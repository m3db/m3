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
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/index"
	"github.com/m3db/m3db/persist"
	idxpersist "github.com/m3db/m3ninx/persist"
	xerrors "github.com/m3db/m3x/errors"
)

const (
	indexFileSetMajorVersion = 1
)

var (
	errIndexFileSetWriterReturnsNoFiles = errors.New(
		"index file set writer returned zero file types")
)

type indexWriter struct {
	opts             Options
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	fdWithDigest     digest.FdWithDigestWriter

	err           error
	blockSize     time.Duration
	start         time.Time
	fileSetType   persist.FileSetType
	snapshotTime  time.Time
	snapshotIndex int
	segments      []writtenIndexSegment

	namespaceDir       string
	checkpointFilePath string
	infoFilePath       string
	digestFilePath     string
}

type writtenIndexSegment struct {
	segmentType  idxpersist.IndexSegmentType
	majorVersion int
	minorVersion int
	metadata     []byte
	files        []writtenIndexSegmentFile
}

type writtenIndexSegmentFile struct {
	segmentFileType idxpersist.IndexSegmentFileType
	digest          uint32
}

// NewIndexWriter returns a new index writer with options.
func NewIndexWriter(opts Options) (IndexFileSetWriter, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	bufferSize := opts.WriterBufferSize()
	return &indexWriter{
		opts:             opts,
		filePathPrefix:   opts.FilePathPrefix(),
		newFileMode:      opts.NewFileMode(),
		newDirectoryMode: opts.NewDirectoryMode(),
		fdWithDigest:     digest.NewFdWithDigestWriter(bufferSize),
	}, nil
}

func (w *indexWriter) Open(opts IndexWriterOpenOptions) error {
	var (
		namespace  = opts.Identifier.Namespace
		blockStart = opts.Identifier.BlockStart
		err        error
	)
	w.err = nil
	w.blockSize = opts.BlockSize
	w.start = blockStart
	w.fileSetType = opts.FileSetType
	w.snapshotTime = opts.Snapshot.SnapshotTime
	w.segments = nil

	switch opts.FileSetType {
	case persist.FileSetSnapshotType:
		w.namespaceDir = NamespaceIndexSnapshotDirPath(w.filePathPrefix, namespace)
		// Can't do this outside of the switch statement because we need to make sure
		// the directory exists before calling NextSnapshotFileIndex
		if err := os.MkdirAll(w.namespaceDir, w.newDirectoryMode); err != nil {
			return err
		}

		// This method is not thread-safe, so its the callers responsibilities that they never
		// try and write two snapshot files for the same block start at the same time.
		w.snapshotIndex, err = NextIndexSnapshotFileIndex(w.filePathPrefix, namespace, blockStart)
		if err != nil {
			return err
		}

		w.checkpointFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, checkpointFileSuffix, w.snapshotIndex)
		w.infoFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, infoFileSuffix, w.snapshotIndex)
		w.digestFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, digestFileSuffix, w.snapshotIndex)
	case persist.FileSetFlushType:
		w.namespaceDir = NamespaceIndexDataDirPath(w.filePathPrefix, namespace)
		if err := os.MkdirAll(w.namespaceDir, w.newDirectoryMode); err != nil {
			return err
		}

		w.checkpointFilePath = filesetPathFromTime(w.namespaceDir, blockStart, checkpointFileSuffix)
		w.infoFilePath = filesetPathFromTime(w.namespaceDir, blockStart, infoFileSuffix)
		w.digestFilePath = filesetPathFromTime(w.namespaceDir, blockStart, digestFileSuffix)
	default:
		return fmt.Errorf("cannot open index writer for fileset type: %s", opts.FileSetType)
	}
	return nil
}

func (w *indexWriter) WriteSegmentFileSet(
	segmentFileSet idxpersist.IndexSegmentFileSetWriter,
) error {
	if w.err != nil {
		return w.err
	}

	segType := segmentFileSet.SegmentType()
	if err := segType.Validate(); err != nil {
		return w.markSegmentWriteError(segType, "", err)
	}

	seg := writtenIndexSegment{
		segmentType:  segType,
		majorVersion: segmentFileSet.MajorVersion(),
		minorVersion: segmentFileSet.MinorVersion(),
		metadata:     segmentFileSet.SegmentMetadata(),
	}

	files := segmentFileSet.Files()
	if len(files) == 0 {
		return w.markSegmentWriteError(segType, "",
			errIndexFileSetWriterReturnsNoFiles)
	}

	idx := len(w.segments)
	for _, segFileType := range files {
		if err := segFileType.Validate(); err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		var filePath string
		switch w.fileSetType {
		case persist.FileSetSnapshotType:
			filePath = snapshotIndexSegmentFilePathFromTimeAndIndex(w.namespaceDir, w.start,
				idx, segFileType, w.snapshotIndex)
		case persist.FileSetFlushType:
			filePath = filesetIndexSegmentFilePathFromTime(w.namespaceDir, w.start,
				idx, segFileType)
		default:
			err := fmt.Errorf("unknown fileset type: %s", w.fileSetType)
			return w.markSegmentWriteError(segType, segFileType, err)
		}
		if FileExists(filePath) {
			err := fmt.Errorf("segment file type already exists at %s", filePath)
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		fd, err := OpenWritable(filePath, w.newFileMode)
		if err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		// Use buffered IO writer to write the file in case the reader
		// returns small chunks of data
		w.fdWithDigest.Reset(fd)
		digest := w.fdWithDigest.Digest()
		writer := bufio.NewWriter(w.fdWithDigest)
		writeErr := segmentFileSet.WriteFile(segFileType, writer)
		err = xerrors.FirstError(writeErr, writer.Flush(), w.fdWithDigest.Close())
		if err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		seg.files = append(seg.files, writtenIndexSegmentFile{
			segmentFileType: segFileType,
			digest:          digest.Sum32(),
		})
	}

	w.segments = append(w.segments, seg)
	return nil
}

func (w *indexWriter) markSegmentWriteError(
	segType idxpersist.IndexSegmentType,
	segFileType idxpersist.IndexSegmentFileType,
	err error,
) error {
	w.err = fmt.Errorf("failed to write segment_type=%s, segment_file_type=%s: %v",
		segType, segFileType, err)
	return w.err
}

func (w *indexWriter) infoFileData() ([]byte, error) {
	info := &index.IndexInfo{
		MajorVersion: indexFileSetMajorVersion,
		BlockStart:   w.start.UnixNano(),
		BlockSize:    int64(w.blockSize),
		FileType:     int64(w.fileSetType),
		SnapshotTime: w.snapshotTime.UnixNano(),
	}
	for _, segment := range w.segments {
		segmentInfo := &index.SegmentInfo{
			SegmentType:  string(segment.segmentType),
			MajorVersion: int64(segment.majorVersion),
			MinorVersion: int64(segment.minorVersion),
			Metadata:     segment.metadata,
		}
		for _, file := range segment.files {
			fileInfo := &index.SegmentFileInfo{
				SegmentFileType: string(file.segmentFileType),
			}
			segmentInfo.Files = append(segmentInfo.Files, fileInfo)
		}
		info.Segments = append(info.Segments, segmentInfo)
	}
	return info.Marshal()
}

func (w *indexWriter) digestsFileData(infoFileData []byte) ([]byte, error) {
	digests := &index.IndexDigests{
		InfoDigest: digest.Checksum(infoFileData),
	}
	for _, segment := range w.segments {
		segmentDigest := &index.SegmentDigest{
			SegmentType: string(segment.segmentType),
		}
		for _, file := range segment.files {
			fileDigest := &index.SegmentFileDigest{
				SegmentFileType: string(file.segmentFileType),
				Digest:          file.digest,
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
	digestBuffer := digest.NewBuffer()
	digestBuffer.WriteDigest(digest.Checksum(digestsFileData))
	return ioutil.WriteFile(w.checkpointFilePath, digestBuffer, w.newFileMode)
}
