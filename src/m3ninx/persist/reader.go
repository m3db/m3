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

	"github.com/m3db/m3ninx/index/segment/fs"
)

// NewSegment returns a new fs.Segment backed by the provided fileset.
func NewSegment(fileset IndexSegmentFileSet, opts fs.NewSegmentOpts) (fs.Segment, error) {
	if t := fileset.SegmentType(); t != FSTIndexSegmentType {
		return nil, fmt.Errorf("unknown segment type: %s", t)
	}
	sd, err := filesetToSegmentData(fileset)
	if err != nil {
		return nil, err
	}
	if err := sd.Validate(); err != nil {
		return nil, err
	}
	return fs.NewSegment(sd, opts)
}

func filesetToSegmentData(fileset IndexSegmentFileSet) (fs.SegmentData, error) {
	var (
		sd = fs.SegmentData{
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
