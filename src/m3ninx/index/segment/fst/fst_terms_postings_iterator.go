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
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	pilosaroaring "github.com/m3dbx/pilosa/roaring"
)

// postingsIterRoaringPoolingConfig uses a configuration that avoids allocating
// any containers in the roaring bitmap, since these roaring bitmaps are backed
// by mmaps and don't have any native containers themselves.
var postingsIterRoaringPoolingConfig = pilosaroaring.ContainerPoolingConfiguration{
	MaxArraySize:                    0,
	MaxRunsSize:                     0,
	AllocateBitmap:                  false,
	MaxCapacity:                     0,
	MaxKeysAndContainersSliceLength: 128 * 10,
}

var _ sgmt.TermsIterator = &fstTermsPostingsIter{}
var _ sgmt.FieldsPostingsListIterator = &fstTermsPostingsIter{}

type fstTermsPostingsIter struct {
	bitmap       *roaring.ReadOnlyBitmap
	legacyBitmap *pilosaroaring.Bitmap
	legacyList   postings.List

	seg          *fsSegment
	termsIter    *fstTermsIter
	currTerm     []byte
	fieldOffsets bool
	err          error
}

func newFSTTermsPostingsIter() *fstTermsPostingsIter {
	var (
		readOnlyBitmap *roaring.ReadOnlyBitmap
		legacyBitmap   *pilosaroaring.Bitmap
	)
	if index.MigrationReadOnlyPostings() {
		readOnlyBitmap = &roaring.ReadOnlyBitmap{}
	} else {
		legacyBitmap = pilosaroaring.NewBitmapWithPooling(postingsIterRoaringPoolingConfig)
	}
	i := &fstTermsPostingsIter{
		bitmap:       readOnlyBitmap,
		legacyBitmap: legacyBitmap,
		legacyList:   roaring.NewPostingsListFromBitmap(legacyBitmap),
	}
	i.clear()
	return i
}

func (f *fstTermsPostingsIter) clear() {
	if index.MigrationReadOnlyPostings() {
		f.bitmap.Reset(nil)
	} else {
		f.legacyBitmap.Reset()
	}
	f.seg = nil
	f.termsIter = nil
	f.currTerm = nil
	f.fieldOffsets = false
	f.err = nil
}

func (f *fstTermsPostingsIter) reset(
	seg *fsSegment,
	termsIter *fstTermsIter,
	fieldOffsets bool,
) {
	f.clear()

	f.seg = seg
	f.termsIter = termsIter
	f.fieldOffsets = fieldOffsets
}

func (f *fstTermsPostingsIter) Next() bool {
	if f.err != nil {
		return false
	}

	next := f.termsIter.Next()
	if !next {
		return false
	}

	f.currTerm = f.termsIter.Current()
	currOffset := f.termsIter.CurrentOffset()

	f.seg.RLock()
	if index.MigrationReadOnlyPostings() {
		f.err = f.seg.unmarshalReadOnlyBitmapNotClosedMaybeFinalizedWithLock(f.bitmap,
			currOffset, f.fieldOffsets)
	} else {
		f.err = f.seg.unmarshalBitmapNotClosedMaybeFinalizedWithLock(f.legacyBitmap,
			currOffset, f.fieldOffsets)
	}
	f.seg.RUnlock()

	return f.err == nil
}

func (f *fstTermsPostingsIter) Current() ([]byte, postings.List) {
	if index.MigrationReadOnlyPostings() {
		return f.currTerm, f.bitmap
	}
	return f.currTerm, f.legacyList
}

func (f *fstTermsPostingsIter) Err() error {
	return f.err
}

func (f *fstTermsPostingsIter) Close() error {
	var err error
	if f.termsIter != nil {
		err = f.termsIter.Close()
	}
	f.clear()
	return err
}
