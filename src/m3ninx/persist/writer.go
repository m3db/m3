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
	"fmt"
	"io"

	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
)

// NewReusableSegmentFileSetWriter returns a new IndexSegmentFileSetWriter for writing
// out the provided Mutable Segment.
func NewReusableSegmentFileSetWriter() (ReusableSegmentFileSetWriter, error) {
	return newReusableSegmentFileSetWriter(fst.NewWriter())
}

func newReusableSegmentFileSetWriter(fsWriter fst.Writer) (ReusableSegmentFileSetWriter, error) {
	return &writer{
		fsWriter: fsWriter,
	}, nil
}

type writer struct {
	fsWriter fst.Writer
}

func (w *writer) Reset(s segment.Segment) error {
	return w.fsWriter.Reset(s)
}

func (w *writer) SegmentType() IndexSegmentType {
	return FSTIndexSegmentType
}

func (w *writer) MajorVersion() int {
	return fst.MajorVersion
}

func (w *writer) MinorVersion() int {
	return fst.MinorVersion
}

func (w *writer) SegmentMetadata() []byte {
	return w.fsWriter.Metadata()
}

func (w *writer) Files() []IndexSegmentFileType {
	// NB(prateek): order is important here. It is the order of files written out,
	// and needs to be maintained as it is below.
	return []IndexSegmentFileType{
		DocumentDataIndexSegmentFileType,
		DocumentIndexIndexSegmentFileType,
		PostingsIndexSegmentFileType,
		FSTTermsIndexSegmentFileType,
		FSTFieldsIndexSegmentFileType,
	}
}

func (w *writer) WriteFile(fileType IndexSegmentFileType, iow io.Writer) error {
	switch fileType {
	case DocumentDataIndexSegmentFileType:
		return w.fsWriter.WriteDocumentsData(iow)
	case DocumentIndexIndexSegmentFileType:
		return w.fsWriter.WriteDocumentsIndex(iow)
	case PostingsIndexSegmentFileType:
		return w.fsWriter.WritePostingsOffsets(iow)
	case FSTFieldsIndexSegmentFileType:
		return w.fsWriter.WriteFSTFields(iow)
	case FSTTermsIndexSegmentFileType:
		return w.fsWriter.WriteFSTTerms(iow)
	}
	return fmt.Errorf("unknown fileType: %s provided", fileType)
}
