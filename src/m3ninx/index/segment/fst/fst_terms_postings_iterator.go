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
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	postingsroaring "github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/pilosa/pilosa/roaring"
)

var postingsIterRoaringPoolingConfig = roaring.ContainerPoolingConfiguration{
	AllocateArray:                   false,
	AllocateRuns:                    false,
	AllocateBitmap:                  false,
	MaxCapacity:                     128,
	MaxKeysAndContainersSliceLength: 128 * 10,
}

type postingsListRetriever interface {
	UnmarshalPostingsListBitmap(b *roaring.Bitmap, offset uint64) error
}

type fstTermsPostingsIter struct {
	bitmap   *roaring.Bitmap
	postings postings.List

	retriever postingsListRetriever
	termsIter *fstTermsIter
	currTerm  []byte
	err       error
}

func newFSTTermsPostingsIter() *fstTermsPostingsIter {
	bitmap := roaring.NewBitmapWithPooling(postingsIterRoaringPoolingConfig)
	return &fstTermsPostingsIter{
		bitmap:   bitmap,
		postings: postingsroaring.NewPostingsListFromBitmap(bitmap),
	}
}

var _ sgmt.TermsIterator = &fstTermsPostingsIter{}

func (f *fstTermsPostingsIter) reset(
	retriever postingsListRetriever,
	termsIter *fstTermsIter,
) {
	f.bitmap.Reset()

	f.retriever = retriever
	f.termsIter = termsIter
	f.currTerm = nil
	f.err = nil
}

func (f *fstTermsPostingsIter) Next() bool {
	if f.err != nil {
		return false
	}

	next := f.termsIter.Next()
	if !next {
		return false
	}

	var offset uint64
	f.currTerm, offset = f.termsIter.CurrentExtended()
	f.err = f.retriever.UnmarshalPostingsListBitmap(f.bitmap, offset)
	if f.err != nil {
		return false
	}

	return true
}

func (f *fstTermsPostingsIter) Current() ([]byte, postings.List) {
	return f.currTerm, f.postings
}

func (f *fstTermsPostingsIter) Err() error {
	return f.err
}

func (f *fstTermsPostingsIter) Close() error {
	err := f.termsIter.Close()
	f.reset(nil, nil)
	return err
}
