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

package fst

import (
	"bytes"
	"testing"

	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

// ToTestSegment returns a FST segment equivalent to the provide mutable segment.
func ToTestSegment(t *testing.T, s sgmt.MutableSegment, opts Options) sgmt.Segment {
	return newFSTSegment(t, s, opts)
}

func newFSTSegment(t *testing.T, s sgmt.MutableSegment, opts Options) sgmt.Segment {
	err := s.Seal()
	require.NoError(t, err)

	w := NewWriter()
	require.NoError(t, w.Reset(s))

	var (
		docsDataBuffer  bytes.Buffer
		docsIndexBuffer bytes.Buffer
		postingsBuffer  bytes.Buffer
		fstTermsBuffer  bytes.Buffer
		fstFieldsBuffer bytes.Buffer
	)

	require.NoError(t, w.WriteDocumentsData(&docsDataBuffer))
	require.NoError(t, w.WriteDocumentsIndex(&docsIndexBuffer))
	require.NoError(t, w.WritePostingsOffsets(&postingsBuffer))
	require.NoError(t, w.WriteFSTTerms(&fstTermsBuffer))
	require.NoError(t, w.WriteFSTFields(&fstFieldsBuffer))

	data := SegmentData{
		MajorVersion:  w.MajorVersion(),
		MinorVersion:  w.MinorVersion(),
		Metadata:      w.Metadata(),
		DocsData:      docsDataBuffer.Bytes(),
		DocsIdxData:   docsIndexBuffer.Bytes(),
		PostingsData:  postingsBuffer.Bytes(),
		FSTTermsData:  fstTermsBuffer.Bytes(),
		FSTFieldsData: fstFieldsBuffer.Bytes(),
	}
	reader, err := NewSegment(data, opts)
	require.NoError(t, err)

	return reader
}
