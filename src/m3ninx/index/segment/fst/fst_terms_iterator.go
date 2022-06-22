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
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/m3dbx/vellum"
)

type newFSTTermsIterOptions struct {
	closeContextOnClose context.Context
}

func newFSTTermsIter(opts newFSTTermsIterOptions) *fstTermsIter {
	iter := new(vellum.FSTIterator)
	i := &fstTermsIter{
		newFSTTermsIterOptions: opts,
		iter:                   iter,
		restoreReusedIter:      iter,
	}
	i.clear()
	return i
}

var _ sgmt.OrderedBytesIterator = &fstTermsIter{}

type fstTermsIter struct {
	newFSTTermsIterOptions
	iter              *vellum.FSTIterator
	restoreReusedIter *vellum.FSTIterator
	opts              fstTermsIterOpts
	err               error
	done              bool
	empty             bool
	firstNext         bool
	current           []byte
	currentValue      uint64
}

type fstTermsIterOpts struct {
	seg         *fsSegment
	fst         *vellum.FST
	fstSearch   *index.CompiledRegex
	finalizeFST bool
	fieldsFST   bool
}

func (o fstTermsIterOpts) Close() error {
	if o.finalizeFST && o.fst != nil {
		return o.fst.Close()
	}
	return nil
}

func (f *fstTermsIter) clear() {
	// NB(rob): If we actually set an explicit iterator
	// when we reset the FST terms iter to use instead
	// of the default iterator then make sure to restore
	// the default re-useable iterator we allocated for
	// the FST terms iterator.
	f.iter = f.restoreReusedIter
	f.opts = fstTermsIterOpts{}
	f.err = nil
	f.done = false
	f.empty = false
	f.firstNext = true
	f.current = nil
	f.currentValue = 0
}

func (f *fstTermsIter) resetContextOnClose(ctx context.Context) {
	f.closeContextOnClose = ctx
}

func (f *fstTermsIter) reset(opts fstTermsIterOpts) {
	f.clear()
	f.opts = opts

	// Consume the first value so we know what to return
	// for "Empty()" method.
	// Given that sometimes a search is passed in we made need
	// to use an iterator that is searching the FST returned from Search()
	// with a regex. If so the only way to know if there
	// results at all is to attempt to iterate the first
	// result.
	var iterErr error
	if regexp := f.opts.fstSearch; regexp != nil {
		// NB(rob): Iterating terms sometimes will use an
		// explicit iterator to limit the set of terms that
		// are iterated based on a regexp search of the FST.
		f.iter, iterErr = f.opts.fst.Search(regexp.FST, regexp.PrefixBegin, regexp.PrefixEnd)
	} else {
		iterErr = f.iter.Reset(f.opts.fst, nil, nil, nil)
	}

	// iterErr will be ErrIteratorDone if no results
	f.handleIterErr(iterErr)

	if f.done {
		// The iterator was empty, record as such to answer the
		// Empty() method call correctly.
		f.empty = true
	}
}

func (f *fstTermsIter) handleIterErr(err error) {
	if err == vellum.ErrIteratorDone {
		f.done = true
	} else {
		f.err = err
	}
}

func (f *fstTermsIter) Next() bool {
	if f.done || f.err != nil {
		return false
	}

	if f.firstNext {
		// Already progressed to first element.
		f.firstNext = false
	} else {
		if err := f.iter.Next(); err != nil {
			f.handleIterErr(err)
			return false
		}
	}

	f.current, f.currentValue = f.iter.Current()
	return true
}

func (f *fstTermsIter) CurrentOffset() uint64 {
	return f.currentValue
}

func (f *fstTermsIter) Empty() bool {
	return f.empty
}

func (f *fstTermsIter) Current() []byte {
	return f.current
}

func (f *fstTermsIter) Err() error {
	return f.err
}

func (f *fstTermsIter) Close() error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(f.iter.Close())
	multiErr = multiErr.Add(f.opts.Close())
	if f.closeContextOnClose != nil {
		f.closeContextOnClose.Close()
		f.closeContextOnClose = nil
	}
	f.clear()
	return multiErr.FinalError()
}
