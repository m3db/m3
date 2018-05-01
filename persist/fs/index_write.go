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
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/index"
	"github.com/m3db/m3db/persist"
)

const (
	indexFileSetMajorVersion = 1
)

var (
	// fileSubTypeRegex allows what can be used for a file sub type,
	// explicitly cannot use "-" as that is our file set file name separator,
	// also we ensure that callers must use lower cased strings.
	fileSubTypeRegex = regexp.MustCompile("^[a-z_]+$")
)

type indexWriter struct {
	opts             Options
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	readerBuf        *bufio.Reader
	fdWithDigest     digest.FdWithDigestWriter

	err          error
	blockSize    time.Duration
	start        time.Time
	snapshotTime time.Time
	fileType     persist.FileSetType
	segments     []writtenIndexSegment

	namespaceDir       string
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
	bufferSize := opts.WriterBufferSize()
	return &indexWriter{
		opts:             opts,
		filePathPrefix:   opts.FilePathPrefix(),
		newFileMode:      opts.NewFileMode(),
		newDirectoryMode: opts.NewDirectoryMode(),
		readerBuf:        bufio.NewReader(nil),
		fdWithDigest:     digest.NewFdWithDigestWriter(bufferSize),
	}, nil
}

func (w *indexWriter) Open(opts IndexWriterOpenOptions) error {
	var (
		nextSnapshotIndex int
		err               error
		namespace         = opts.Identifier.Namespace
		blockStart        = opts.Identifier.BlockStart
	)
	w.err = nil
	w.blockSize = opts.BlockSize
	w.start = blockStart
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
		nextSnapshotIndex, err = NextIndexSnapshotFileIndex(w.filePathPrefix, namespace, blockStart)
		if err != nil {
			return err
		}

		w.fileType = persist.FileSetSnapshotType
		w.checkpointFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, checkpointFileSuffix, nextSnapshotIndex)
		w.infoFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, infoFileSuffix, nextSnapshotIndex)
		w.digestFilePath = snapshotPathFromTimeAndIndex(w.namespaceDir, blockStart, digestFileSuffix, nextSnapshotIndex)
	case persist.FileSetFlushType:
		w.namespaceDir = NamespaceIndexDataDirPath(w.filePathPrefix, namespace)
		if err := os.MkdirAll(w.namespaceDir, w.newDirectoryMode); err != nil {
			return err
		}

		w.fileType = persist.FileSetFlushType
		w.checkpointFilePath = filesetPathFromTime(w.namespaceDir, blockStart, checkpointFileSuffix)
		w.infoFilePath = filesetPathFromTime(w.namespaceDir, blockStart, infoFileSuffix)
		w.digestFilePath = filesetPathFromTime(w.namespaceDir, blockStart, digestFileSuffix)
	}
	return nil
}

func (w *indexWriter) WriteSegmentFileSet(segmentFileSet IndexSegmentFileSet) error {
	// Always close the files
	defer func() {
		for _, file := range segmentFileSet.Files() {
			file.Close()
		}
	}()

	if w.err != nil {
		return w.err
	}

	segType := string(segmentFileSet.SegmentType())
	if segType == "" || !fileSubTypeRegex.MatchString(segType) {
		err := fmt.Errorf("invalid segment type must match pattern=%s",
			fileSubTypeRegex.String())
		return w.markSegmentWriteError(segType, "", err)
	}

	seg := writtenIndexSegment{
		segmentType:  segmentFileSet.SegmentType(),
		majorVersion: segmentFileSet.MajorVersion(),
		minorVersion: segmentFileSet.MinorVersion(),
		metadata:     segmentFileSet.SegmentMetadata(),
	}

	idx := len(w.segments)
	for _, file := range segmentFileSet.Files() {
		segFileType := string(file.SegmentFileType())
		if segFileType == "" || !fileSubTypeRegex.MatchString(segFileType) {
			err := fmt.Errorf("invalid segment file type must match pattern=%s",
				fileSubTypeRegex.String())
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		filePath := filesetIndexSegmentFilePathFromTime(w.namespaceDir, w.start,
			idx, IndexSegmentFileType(segFileType))
		if FileExists(filePath) {
			err := fmt.Errorf("segment file type already exists at %s", filePath)
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		fd, err := OpenWritable(filePath, w.newFileMode)
		if err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		// Use buffered IO reader to write the file in case the reader
		// returns small chunks of data
		w.readerBuf.Reset(file)
		w.fdWithDigest.Reset(fd)
		digest := w.fdWithDigest.Digest()
		if _, err := w.readerBuf.WriteTo(w.fdWithDigest); err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}
		if err := w.fdWithDigest.Close(); err != nil {
			return w.markSegmentWriteError(segType, segFileType, err)
		}

		seg.files = append(seg.files, writtenIndexSegmentFile{
			segmentFileType: file.SegmentFileType(),
			digest:          digest.Sum32(),
		})
	}

	w.segments = append(w.segments, seg)
	return nil
}

func (w *indexWriter) markSegmentWriteError(
	segType, segFileType string,
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
		FileType:     int64(w.fileType),
		SnapshotTime: w.snapshotTime.UnixNano(),
	}
	for _, segment := range w.segments {
		segmentInfo := &index.IndexInfo_SegmentInfo{
			Type:         string(segment.segmentType),
			MajorVersion: int64(segment.majorVersion),
			MinorVersion: int64(segment.minorVersion),
			Metadata:     segment.metadata,
		}
		for _, file := range segment.files {
			fileInfo := &index.IndexInfo_SegmentFileInfo{
				FileType: string(file.segmentFileType),
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
	digestBuffer := digest.NewBuffer()
	digestBuffer.WriteDigest(digest.Checksum(digestsFileData))
	return ioutil.WriteFile(w.checkpointFilePath, digestBuffer, w.newFileMode)
}
