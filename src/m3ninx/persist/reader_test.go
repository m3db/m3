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
	"testing"

	"github.com/m3db/m3ninx/index/segment/fs"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestReaderValidateType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(IndexSegmentType("random"))
	_, err := NewSegment(fset, fs.NewSegmentOpts{})
	require.Error(t, err)
}

func TestReaderValidateDataSlices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fs.MajorVersion)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})
	fset.EXPECT().Files().Return(nil)

	_, err := NewSegment(fset, fs.NewSegmentOpts{})
	require.Error(t, err)
}

func TestReaderValidateByteAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fs.MajorVersion)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(DocumentDataIndexSegmentFileType)
	docsDataFile.EXPECT().Bytes().Return(nil, fmt.Errorf("random"))
	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile})

	_, err := NewSegment(fset, fs.NewSegmentOpts{})
	require.Error(t, err)
}

func TestReaderValidateSegmentFileType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fs.MajorVersion)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(IndexSegmentFileType("rand"))
	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile})

	_, err := NewSegment(fset, fs.NewSegmentOpts{})
	require.Error(t, err)
}

func TestReaderValidateAllByteAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().MajorVersion().Return(fs.MajorVersion)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(DocumentDataIndexSegmentFileType)
	docsDataFile.EXPECT().Bytes().Return([]byte{}, nil)

	docsIdxFile := NewMockIndexSegmentFile(ctrl)
	docsIdxFile.EXPECT().SegmentFileType().Return(DocumentIndexIndexSegmentFileType)
	docsIdxFile.EXPECT().Bytes().Return([]byte{}, nil)

	postingsFile := NewMockIndexSegmentFile(ctrl)
	postingsFile.EXPECT().SegmentFileType().Return(PostingsIndexSegmentFileType)
	postingsFile.EXPECT().Bytes().Return([]byte{}, nil)

	fstFieldsFile := NewMockIndexSegmentFile(ctrl)
	fstFieldsFile.EXPECT().SegmentFileType().Return(FSTFieldsIndexSegmentFileType)
	fstFieldsFile.EXPECT().Bytes().Return([]byte{}, nil)

	fstTermsFile := NewMockIndexSegmentFile(ctrl)
	fstTermsFile.EXPECT().SegmentFileType().Return(FSTTermsIndexSegmentFileType)
	fstTermsFile.EXPECT().Bytes().Return([]byte{}, nil)

	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile, docsIdxFile,
		postingsFile,
		fstFieldsFile,
		fstTermsFile})

	sd, err := filesetToSegmentData(fset)
	require.NoError(t, err)

	require.NoError(t, sd.Validate())
}
