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
	"io"
	"testing"

	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func newTestWriter(t *testing.T, ctrl *gomock.Controller) (
	*fst.MockWriter,
	ReusableSegmentFileSetWriter,
) {
	w := fst.NewMockWriter(ctrl)
	writer, err := newReusableSegmentFileSetWriter(w)
	require.NoError(t, err)
	return w, writer
}

func TestWriterFiles(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	_, w := newTestWriter(t, ctrl)
	require.Equal(t, w.Files(), []IndexSegmentFileType{
		DocumentDataIndexSegmentFileType,
		DocumentIndexIndexSegmentFileType,
		PostingsIndexSegmentFileType,
		FSTTermsIndexSegmentFileType,
		FSTFieldsIndexSegmentFileType,
	})
}

func TestWriterWriteFile(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	var iow io.Writer
	fsWriter, w := newTestWriter(t, ctrl)

	fsWriter.EXPECT().WriteDocumentsData(iow).Return(nil)
	require.NoError(t, w.WriteFile(DocumentDataIndexSegmentFileType, iow))

	fsWriter.EXPECT().WriteDocumentsIndex(iow).Return(nil)
	require.NoError(t, w.WriteFile(DocumentIndexIndexSegmentFileType, iow))

	fsWriter.EXPECT().WritePostingsOffsets(iow).Return(nil)
	require.NoError(t, w.WriteFile(PostingsIndexSegmentFileType, iow))

	fsWriter.EXPECT().WriteFSTFields(iow).Return(nil)
	require.NoError(t, w.WriteFile(FSTFieldsIndexSegmentFileType, iow))

	fsWriter.EXPECT().WriteFSTTerms(iow).Return(nil)
	require.NoError(t, w.WriteFile(FSTTermsIndexSegmentFileType, iow))
}
