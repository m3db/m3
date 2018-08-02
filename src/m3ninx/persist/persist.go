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

	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/x/mmap"
)

// TransformAndMmap transforms the given segment into a FST backed by
// mmap'd bytes.
func TransformAndMmap(seg sgmt.Segment, opts fst.Options) (sgmt.Segment, error) {
	writer, err := NewReusableSegmentFileSetWriter()
	if err != nil {
		return nil, err
	}

	if err := writer.Reset(seg); err != nil {
		return nil, err
	}

	success := false
	fstData := &fstSegmentMetadata{
		major:    writer.MajorVersion(),
		minor:    writer.MinorVersion(),
		metadata: append([]byte{}, writer.SegmentMetadata()...),
	}
	// cleanup incase we run into issues
	defer func() {
		if !success {
			for _, f := range fstData.files {
				f.Close()
			}
		}
	}()

	var bytesWriter bytes.Buffer
	for _, f := range writer.Files() {
		bytesWriter.Reset()
		if err := writer.WriteFile(f, &bytesWriter); err != nil {
			return nil, err
		}
		fileBytes := bytesWriter.Bytes()
		// memcpy bytes -> new mmap region to hide from the GC
		mmapedResult, err := mmap.Bytes(int64(len(fileBytes)), mmap.Options{
			Read:  true,
			Write: true, // TODO(prateek): pass down huge TLB constraints here?
		})
		if err != nil {
			return nil, err
		}
		copy(mmapedResult.Result, fileBytes)
		fstData.files = append(fstData.files, NewMmapedIndexSegmentFile(f, nil, mmapedResult.Result))
	}

	// NB: need to mark success here as the NewSegment call assumes ownership of
	// the provided bytes regardless of success/failure.
	success = true
	fstSegment, err := NewSegment(fstData, opts)
	if err != nil {
		return nil, err
	}

	return fstSegment, nil

}

type fstSegmentMetadata struct {
	major    int
	minor    int
	metadata []byte
	files    []IndexSegmentFile
}

var _ IndexSegmentFileSet = &fstSegmentMetadata{}

func (f *fstSegmentMetadata) SegmentType() IndexSegmentType { return FSTIndexSegmentType }
func (f *fstSegmentMetadata) MajorVersion() int             { return f.major }
func (f *fstSegmentMetadata) MinorVersion() int             { return f.minor }
func (f *fstSegmentMetadata) SegmentMetadata() []byte       { return f.metadata }
func (f *fstSegmentMetadata) Files() []IndexSegmentFile     { return f.files }
