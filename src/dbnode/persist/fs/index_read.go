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
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/proto/index"
	"github.com/m3db/m3/src/dbnode/persist"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/mmap"
	xlog "github.com/m3db/m3x/log"
)

type indexReader struct {
	opts           Options
	filePathPrefix string
	hugePagesOpts  mmap.HugeTLBOptions
	logger         xlog.Logger

	namespaceDir string
	start        time.Time
	fileSetType  persist.FileSetType
	volumeIndex  int

	currIdx                int
	info                   index.IndexInfo
	expectedDigest         index.IndexDigests
	expectedDigestOfDigest uint32
	readDigests            indexReaderReadDigests
}

type indexReaderReadDigests struct {
	infoFileDigest    uint32
	digestsFileDigest uint32
	segments          []indexReaderReadSegmentDigests
}

type indexReaderReadSegmentDigests struct {
	segmentType idxpersist.IndexSegmentType
	files       []indexReaderReadSegmentFileDigest
}

type indexReaderReadSegmentFileDigest struct {
	segmentFileType idxpersist.IndexSegmentFileType
	digest          uint32
}

// NewIndexReader returns a new index reader with options.
func NewIndexReader(opts Options) (IndexFileSetReader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r := new(indexReader)
	r.reset(opts)
	return r, nil
}

func (r *indexReader) reset(opts Options) {
	*r = indexReader{}
	r.opts = opts
	r.filePathPrefix = opts.FilePathPrefix()
	r.hugePagesOpts = mmap.HugeTLBOptions{
		Enabled:   opts.MmapEnableHugeTLB(),
		Threshold: opts.MmapHugeTLBThreshold(),
	}
	r.logger = opts.InstrumentOptions().Logger()
}

func (r *indexReader) Open(
	opts IndexReaderOpenOptions,
) (IndexReaderOpenResult, error) {
	var result IndexReaderOpenResult

	// NB(r): so the reader can be reused.
	r.reset(r.opts)

	var (
		namespace          = opts.Identifier.Namespace
		checkpointFilepath string
		infoFilepath       string
		digestFilepath     string
	)
	r.start = opts.Identifier.BlockStart
	r.fileSetType = opts.FileSetType
	r.volumeIndex = opts.Identifier.VolumeIndex
	switch opts.FileSetType {
	case persist.FileSetSnapshotType:
		r.namespaceDir = NamespaceIndexSnapshotDirPath(r.filePathPrefix, namespace)
	case persist.FileSetFlushType:
		r.namespaceDir = NamespaceIndexDataDirPath(r.filePathPrefix, namespace)
	default:
		return result, fmt.Errorf("cannot open index reader for fileset type: %s", opts.FileSetType)
	}
	checkpointFilepath = filesetPathFromTimeAndIndex(r.namespaceDir, r.start, r.volumeIndex, checkpointFileSuffix)
	infoFilepath = filesetPathFromTimeAndIndex(r.namespaceDir, r.start, r.volumeIndex, infoFileSuffix)
	digestFilepath = filesetPathFromTimeAndIndex(r.namespaceDir, r.start, r.volumeIndex, digestFileSuffix)

	// If there is no checkpoint file, don't read the index files.
	if err := r.readCheckpointFile(checkpointFilepath); err != nil {
		return result, err
	}
	if err := r.readDigestsFile(digestFilepath); err != nil {
		return result, err
	}
	if err := r.readInfoFile(infoFilepath); err != nil {
		return result, err
	}
	result.Shards = make(map[uint32]struct{}, len(r.info.Shards))
	for _, shard := range r.info.Shards {
		result.Shards[shard] = struct{}{}
	}
	return result, nil
}

func (r *indexReader) readCheckpointFile(filePath string) error {
	exists, err := FileExists(filePath)
	if err != nil {
		return err
	}
	if !exists {
		return ErrCheckpointFileNotFound
	}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest.Buffer(data).ReadDigest()
	return nil
}

func (r *indexReader) readDigestsFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	r.readDigests.digestsFileDigest = digest.Checksum(data)
	if err := r.validateDigestsFileDigest(); err != nil {
		return err
	}
	return r.expectedDigest.Unmarshal(data)
}

func (r *indexReader) readInfoFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	r.readDigests.infoFileDigest = digest.Checksum(data)
	if r.readDigests.infoFileDigest != r.expectedDigest.InfoDigest {
		return fmt.Errorf("read info file checksum bad: expected=%d, actual=%d",
			r.expectedDigest.InfoDigest, r.readDigests.infoFileDigest)
	}
	return r.info.Unmarshal(data)
}

func (r *indexReader) SegmentFileSets() int {
	return len(r.info.Segments)
}

func (r *indexReader) ReadSegmentFileSet() (
	idxpersist.IndexSegmentFileSet,
	error,
) {
	if r.currIdx >= len(r.info.Segments) {
		return nil, io.EOF
	}

	var (
		segment = r.info.Segments[r.currIdx]
		result  = readableIndexSegmentFileSet{
			info:  segment,
			files: make([]idxpersist.IndexSegmentFile, 0, len(segment.Files)),
		}
		digests = indexReaderReadSegmentDigests{
			segmentType: idxpersist.IndexSegmentType(segment.SegmentType),
		}
	)
	closeFiles := func() {
		for _, file := range result.files {
			file.Close()
		}
	}
	for _, file := range segment.Files {
		segFileType := idxpersist.IndexSegmentFileType(file.SegmentFileType)

		var filePath string
		switch r.fileSetType {
		case persist.FileSetSnapshotType:
			filePath = snapshotIndexSegmentFilePathFromTimeAndIndex(r.namespaceDir, r.start, r.volumeIndex,
				r.currIdx, segFileType)
		case persist.FileSetFlushType:
			filePath = filesetIndexSegmentFilePathFromTime(r.namespaceDir, r.start, r.volumeIndex,
				r.currIdx, segFileType)
		default:
			closeFiles()
			return nil, fmt.Errorf("unknown fileset type: %s", r.fileSetType)
		}

		var (
			fd    *os.File
			bytes []byte
		)
		mmapResult, err := mmap.Files(os.Open, map[string]mmap.FileDesc{
			filePath: mmap.FileDesc{
				File:    &fd,
				Bytes:   &bytes,
				Options: mmap.Options{Read: true, HugeTLB: r.hugePagesOpts},
			},
		})
		if err != nil {
			closeFiles()
			return nil, err
		}

		if warning := mmapResult.Warning; warning != nil {
			r.logger.Warnf("warning while mmapping files in reader: %s",
				warning.Error())
		}

		file := idxpersist.NewMmapedIndexSegmentFile(segFileType, fd, bytes)
		result.files = append(result.files, file)
		digests.files = append(digests.files, indexReaderReadSegmentFileDigest{
			segmentFileType: segFileType,
			digest:          digest.Checksum(bytes),
		})
	}

	r.currIdx++
	r.readDigests.segments = append(r.readDigests.segments, digests)
	return result, nil
}

func (r *indexReader) Validate() error {
	if err := r.validateDigestsFileDigest(); err != nil {
		return err
	}
	if err := r.validateInfoFileDigest(); err != nil {
		return err
	}
	for i, segment := range r.info.Segments {
		for j := range segment.Files {
			if err := r.validateSegmentFileDigest(i, j); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *indexReader) validateDigestsFileDigest() error {
	if r.readDigests.digestsFileDigest != r.expectedDigestOfDigest {
		return fmt.Errorf("read digests file checksum bad: expected=%d, actual=%d",
			r.expectedDigestOfDigest, r.readDigests.digestsFileDigest)
	}
	return nil
}

func (r *indexReader) validateInfoFileDigest() error {
	if r.readDigests.infoFileDigest != r.expectedDigest.InfoDigest {
		return fmt.Errorf("read info file checksum bad: expected=%d, actual=%d",
			r.expectedDigest.InfoDigest, r.readDigests.infoFileDigest)
	}
	return nil
}

func (r *indexReader) validateSegmentFileDigest(segmentIdx, fileIdx int) error {
	if segmentIdx >= len(r.readDigests.segments) {
		return fmt.Errorf(
			"have not read correct number of segments to validate segment %d checksums: "+
				"need=%d, actual=%d",
			segmentIdx, segmentIdx+1, len(r.readDigests.segments))
	}
	if segmentIdx >= len(r.expectedDigest.SegmentDigests) {
		return fmt.Errorf(
			"have not read digest files correctly to validate segment %d checksums: "+
				"need=%d, actual=%d",
			segmentIdx, segmentIdx+1, len(r.expectedDigest.SegmentDigests))
	}

	if fileIdx >= len(r.readDigests.segments[segmentIdx].files) {
		return fmt.Errorf(
			"have not read correct number of segment files to validate segment %d checksums: "+
				"need=%d, actual=%d",
			segmentIdx, fileIdx+1, len(r.readDigests.segments[segmentIdx].files))
	}
	if fileIdx >= len(r.expectedDigest.SegmentDigests[segmentIdx].Files) {
		return fmt.Errorf(
			"have not read correct number of segment files to validate segment %d checksums: "+
				"need=%d, actual=%d",
			segmentIdx, fileIdx+1, len(r.expectedDigest.SegmentDigests[segmentIdx].Files))
	}

	expected := r.expectedDigest.SegmentDigests[segmentIdx].Files[fileIdx].Digest
	actual := r.readDigests.segments[segmentIdx].files[fileIdx].digest
	if actual != expected {
		return fmt.Errorf("read segment file %d for segment %d checksum bad: expected=%d, actual=%d",
			segmentIdx, fileIdx, expected, actual)
	}
	return nil
}

func (r *indexReader) Close() error {
	r.reset(r.opts)
	return nil
}

// NB(r): to force the type to compile to match interface IndexSegmentFileSet
var _ IndexSegmentFileSet = readableIndexSegmentFileSet{}

type readableIndexSegmentFileSet struct {
	info  *index.SegmentInfo
	files []idxpersist.IndexSegmentFile
}

func (s readableIndexSegmentFileSet) SegmentType() idxpersist.IndexSegmentType {
	return idxpersist.IndexSegmentType(s.info.SegmentType)
}

func (s readableIndexSegmentFileSet) MajorVersion() int {
	return int(s.info.MajorVersion)
}

func (s readableIndexSegmentFileSet) MinorVersion() int {
	return int(s.info.MinorVersion)
}

func (s readableIndexSegmentFileSet) SegmentMetadata() []byte {
	return s.info.Metadata
}

func (s readableIndexSegmentFileSet) Files() []idxpersist.IndexSegmentFile {
	return s.files
}
