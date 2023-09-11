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

	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/x/mmap"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func TestReaderValidateType(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(IndexSegmentType("random"))
	fset.EXPECT().Files().Return(nil).AnyTimes()
	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateErrorCloses(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	file := NewMockIndexSegmentFile(ctrl)
	file.EXPECT().Close()
	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(IndexSegmentType("random"))
	fset.EXPECT().Files().Return([]IndexSegmentFile{file}).AnyTimes()
	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateDataSlices(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fst.CurrentVersion.Major)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})
	fset.EXPECT().Files().Return(nil).AnyTimes()

	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateByteAccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fst.CurrentVersion.Major)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(DocumentDataIndexSegmentFileType)
	docsDataFile.EXPECT().Mmap().Return(mmap.Descriptor{}, fmt.Errorf("random"))
	docsDataFile.EXPECT().Close()
	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile}).AnyTimes()

	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateDoesNotCloseAllOnBadByteAccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fst.CurrentVersion.Major)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(DocumentDataIndexSegmentFileType)
	docsDataFile.EXPECT().Close()
	docsDataFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)

	docsIdxFile := NewMockIndexSegmentFile(ctrl)
	docsIdxFile.EXPECT().SegmentFileType().Return(DocumentIndexIndexSegmentFileType)
	docsIdxFile.EXPECT().Close()
	docsIdxFile.EXPECT().Mmap().Return(mmap.Descriptor{}, fmt.Errorf("random"))

	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile, docsIdxFile}).AnyTimes()

	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateSegmentFileType(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fst.CurrentVersion.Major)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(IndexSegmentFileType("rand"))
	docsDataFile.EXPECT().Close()
	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile}).AnyTimes()

	_, err := NewSegment(fset, nil)
	require.Error(t, err)
}

func TestReaderValidateAllByteAccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fset := NewMockIndexSegmentFileSet(ctrl)
	fset.EXPECT().SegmentType().Return(FSTIndexSegmentType)
	fset.EXPECT().MajorVersion().Return(fst.CurrentVersion.Major)
	fset.EXPECT().MinorVersion().Return(1)
	fset.EXPECT().SegmentMetadata().Return([]byte{})

	docsDataFile := NewMockIndexSegmentFile(ctrl)
	docsDataFile.EXPECT().SegmentFileType().Return(DocumentDataIndexSegmentFileType)
	docsDataFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)
	docsDataFile.EXPECT().Close()

	docsIdxFile := NewMockIndexSegmentFile(ctrl)
	docsIdxFile.EXPECT().SegmentFileType().Return(DocumentIndexIndexSegmentFileType)
	docsIdxFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)
	docsIdxFile.EXPECT().Close()

	postingsFile := NewMockIndexSegmentFile(ctrl)
	postingsFile.EXPECT().SegmentFileType().Return(PostingsIndexSegmentFileType)
	postingsFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)
	postingsFile.EXPECT().Close()

	fstFieldsFile := NewMockIndexSegmentFile(ctrl)
	fstFieldsFile.EXPECT().SegmentFileType().Return(FSTFieldsIndexSegmentFileType)
	fstFieldsFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)
	fstFieldsFile.EXPECT().Close()

	fstTermsFile := NewMockIndexSegmentFile(ctrl)
	fstTermsFile.EXPECT().SegmentFileType().Return(FSTTermsIndexSegmentFileType)
	fstTermsFile.EXPECT().Mmap().Return(mmap.Descriptor{}, nil)
	fstTermsFile.EXPECT().Close()

	fset.EXPECT().Files().Return([]IndexSegmentFile{docsDataFile, docsIdxFile,
		postingsFile,
		fstFieldsFile,
		fstTermsFile}).AnyTimes()

	_, err := NewSegment(fset, nil)
	// due to empty bytes being passed
	require.Error(t, err)
}
