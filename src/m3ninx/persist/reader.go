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

package persist

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/x"
	"github.com/m3db/m3/src/x/mmap"
)

// NewSegment returns a new fst.Segment backed by the provided fileset.
// NB: this method takes ownership of the provided fileset files, in case of both errors,
// and success. i.e. users are not expected to call Close on any of the provided fileset.Files()
// after invoking this function.
func NewSegment(fileset IndexSegmentFileSet, opts fst.Options) (fst.Segment, error) {
	success := false
	safeCloser := newSafeIndexSegmentFileSetCloser(fileset)
	defer func() {
		if !success {
			safeCloser.Close()
		}
	}()

	if t := fileset.SegmentType(); t != FSTIndexSegmentType {
		return nil, fmt.Errorf("unknown segment type: %s", t)
	}

	sd, err := filesetToSegmentData(fileset)
	if err != nil {
		return nil, err
	}
	sd.Closer = safeCloser

	segment, err := fst.NewSegment(sd, opts)
	if err != nil {
		return nil, err
	}

	// indicate we don't need to close files in the defer above.
	success = true

	// segment assumes ownership of the safeCloser at this point.
	return segment, nil
}

func filesetToSegmentData(fileset IndexSegmentFileSet) (fst.SegmentData, error) {
	var (
		sd = fst.SegmentData{
			MajorVersion: fileset.MajorVersion(),
			MinorVersion: fileset.MinorVersion(),
			Metadata:     fileset.SegmentMetadata(),
		}
		err error
	)

	for _, f := range fileset.Files() {
		fileType := f.SegmentFileType()
		switch fileType {
		case DocumentDataIndexSegmentFileType:
			sd.DocsData, err = f.Bytes()
			if err != nil {
				return sd, err
			}
		case DocumentIndexIndexSegmentFileType:
			sd.DocsIdxData, err = f.Bytes()
			if err != nil {
				return sd, err
			}
		case PostingsIndexSegmentFileType:
			sd.PostingsData, err = f.Bytes()
			if err != nil {
				return sd, err
			}
		case FSTFieldsIndexSegmentFileType:
			sd.FSTFieldsData, err = f.Bytes()
			if err != nil {
				return sd, err
			}
		case FSTTermsIndexSegmentFileType:
			sd.FSTTermsData, err = f.Bytes()
			if err != nil {
				return sd, err
			}
		default:
			return sd, fmt.Errorf("unknown fileType: %s provided", fileType)
		}
	}

	return sd, nil
}

func newSafeIndexSegmentFileSetCloser(fileset IndexSegmentFileSet) io.Closer {
	closers := make([]io.Closer, 0, len(fileset.Files()))
	for _, f := range fileset.Files() {
		closers = append(closers, f)
	}
	return x.NewSafeMultiCloser(closers...)
}

// NewMmapedIndexSegmentFile returns an IndexSegmentFile backed by the provided bytes and FD.
// The returned object assumes ownership of the input fd, and mmap-ed bytes.
func NewMmapedIndexSegmentFile(
	fileType IndexSegmentFileType,
	fd *os.File,
	bytesMmap []byte,
) IndexSegmentFile {
	r := &readableIndexSegmentFileMmap{
		fileType:  fileType,
		fd:        fd,
		bytesMmap: bytesMmap,
	}
	r.reader.Reset(r.bytesMmap)
	return r
}

type readableIndexSegmentFileMmap struct {
	fileType  IndexSegmentFileType
	fd        *os.File
	bytesMmap []byte
	reader    bytes.Reader
}

var _ IndexSegmentFile = &readableIndexSegmentFileMmap{}

func (f *readableIndexSegmentFileMmap) SegmentFileType() IndexSegmentFileType {
	return f.fileType
}

func (f *readableIndexSegmentFileMmap) Bytes() ([]byte, error) {
	return f.bytesMmap, nil
}

func (f *readableIndexSegmentFileMmap) Read(b []byte) (int, error) {
	return f.reader.Read(b)
}

func (f *readableIndexSegmentFileMmap) Close() error {
	// Be sure to close the mmap before the file
	if f.bytesMmap != nil {
		if err := mmap.Munmap(f.bytesMmap); err != nil {
			return err
		}
		f.bytesMmap = nil
	}
	if f.fd != nil {
		if err := f.fd.Close(); err != nil {
			return err
		}
		f.fd = nil
	}
	f.reader.Reset(nil)
	return nil
}
