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
	xerrors "github.com/m3db/m3x/errors"
)

type postingsListRetriever interface {
	PostingsListAtOffset(postingsOffset uint64) (postings.List, error)
}

type fstTermsPostingsIter struct {
	retriever    postingsListRetriever
	termsIter    *fstTermsIter
	currTerm     []byte
	currPostings postings.List
	err          error
}

func newFSTTermsPostingsIter(
	retriever postingsListRetriever,
	termsIter *fstTermsIter,
) *fstTermsPostingsIter {
	return &fstTermsPostingsIter{
		retriever: retriever,
		termsIter: termsIter,
	}
}

var _ sgmt.TermsIterator = &fstTermsPostingsIter{}

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
	f.currPostings, f.err = f.retriever.PostingsListAtOffset(offset)
	if f.err != nil {
		return false
	}

	return true
}

func (f *fstTermsPostingsIter) Current() ([]byte, postings.List) {
	return f.currTerm, f.currPostings
}

func (f *fstTermsPostingsIter) Err() error {
	return f.err
}

func (f *fstTermsPostingsIter) Close() error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(f.termsIter.Close())
	return multiErr.FinalError()
}
