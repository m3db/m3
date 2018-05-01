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
	"io/ioutil"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/index"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/x/mmap"
)

type indexReader struct {
	opts           Options
	filePathPrefix string
	hugePagesOpts  mmap.HugeTLBOptions

	start     time.Time
	blockSize time.Duration

	currIdx                int
	info                   *index.IndexInfo
	expectedDigests        *index.IndexDigests
	expectedDigestOfDigest uint32
}

// NewIndexReader returns a new index reader with options.
func NewIndexReader(opts Options) (IndexFileSetReader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &indexReader{
		opts:           opts,
		filePathPrefix: opts.FilePathPrefix(),
		hugePagesOpts: mmap.HugeTLBOptions{
			Enabled:   opts.MmapEnableHugeTLB(),
			Threshold: opts.MmapHugeTLBThreshold(),
		},
	}, nil
}

func (r *indexReader) Open(opts IndexReaderOpenOptions) error {
	var (
		namespace          = opts.Identifier.Namespace
		blockStart         = opts.Identifier.BlockStart
		snapshotIndex      = opts.Identifier.Index
		namespaceDir       string
		checkpointFilepath string
		infoFilepath       string
		digestFilepath     string
	)
	r.currIdx = 0
	switch opts.FilesetType {
	case persist.FileSetSnapshotType:
		namespaceDir = NamespaceIndexSnapshotDirPath(r.filePathPrefix, namespace)
		checkpointFilepath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, checkpointFileSuffix, snapshotIndex)
		infoFilepath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, infoFileSuffix, snapshotIndex)
		digestFilepath = snapshotPathFromTimeAndIndex(namespaceDir, blockStart, digestFileSuffix, snapshotIndex)
	case persist.FileSetFlushType:
		namespaceDir = NamespaceIndexDataDirPath(r.filePathPrefix, namespace)
		checkpointFilepath = filesetPathFromTime(namespaceDir, blockStart, checkpointFileSuffix)
		infoFilepath = filesetPathFromTime(namespaceDir, blockStart, infoFileSuffix)
		digestFilepath = filesetPathFromTime(namespaceDir, blockStart, digestFileSuffix)
	default:
		return fmt.Errorf("unable to open reader with fileset type: %s", opts.FilesetType)
	}

	// If there is no checkpoint file, don't read the index files.
	if err := r.readCheckpointFile(checkpointFilepath); err != nil {
		return err
	}
	if err := r.readDigests(digestFilepath); err != nil {
		return err
	}
	if err := r.readInfo(infoFilepath); err != nil {
		return err
	}
	return nil
}

func (r *indexReader) readCheckpointFile(filePath string) error {
	if !FileExists(filePath) {
		return ErrCheckpointFileNotFound
	}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest.Checksum(data)
	return nil
}

func (r *indexReader) readDigests(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	checksum := digest.Checksum(data)
	if checksum != r.expectedDigestOfDigest {
		return fmt.Errorf("read digests file checksum bad: expected=%d, actual=%d",
			r.expectedDigestOfDigest, checksum)
	}
	return r.expectedDigests.Unmarshal(data)
}

func (r *indexReader) readInfo(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	checksum := digest.Checksum(data)
	if checksum != r.expectedDigests.InfoDigest {
		return fmt.Errorf("read info file checksum bad: expected=%d, actual=%d",
			r.expectedDigests.InfoDigest, checksum)
	}
	return r.info.Unmarshal(data)
}

func (r *indexReader) ReadSegmentFileSet() (IndexSegmentFileSet, error) {
	return nil, nil
}

func (r *indexReader) Validate() error {
	return nil
}

func (r *indexReader) Close() error {
	return nil
}
