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
	xerrors "github.com/m3db/m3x/errors"

	"github.com/couchbase/vellum"
)

type fstTermsIterOpts struct {
	fst         *vellum.FST
	finalizeFST bool
}

func (o fstTermsIterOpts) Close() error {
	if o.finalizeFST {
		return o.fst.Close()
	}
	return nil
}

func newFSTTermsIter() *fstTermsIter {
	return &fstTermsIter{}
}

type fstTermsIter struct {
	iter         vellum.FSTIterator
	opts         fstTermsIterOpts
	err          error
	done         bool
	firstNext    bool
	current      []byte
	currentValue uint64
}

var _ sgmt.OrderedBytesIterator = &fstTermsIter{}

func (f *fstTermsIter) reset(opts fstTermsIterOpts) {
	f.clear()
	f.opts = opts

	if err := f.iter.Reset(opts.fst, nil, nil, nil); err != nil {
		f.handleIterErr(err)
	}
}

func (f *fstTermsIter) handleIterErr(err error) {
	if err == vellum.ErrIteratorDone {
		f.done = true
	} else {
		f.err = err
	}
}

func (f *fstTermsIter) clear() {
	f.iter = vellum.FSTIterator{}
	f.opts = fstTermsIterOpts{}
	f.err = nil
	f.done = false
	f.firstNext = true
	f.current = nil
	f.currentValue = 0
}

func (f *fstTermsIter) Next() bool {
	if f.done || f.err != nil {
		return false
	}

	if f.firstNext {
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

func (f *fstTermsIter) CurrentExtended() ([]byte, uint64) {
	return f.current, f.currentValue
}

func (f *fstTermsIter) Current() []byte {
	return f.current
}

func (f *fstTermsIter) Err() error {
	return f.err
}

func (f *fstTermsIter) Len() int {
	return f.opts.fst.Len()
}

func (f *fstTermsIter) Close() error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(f.iter.Close())
	multiErr = multiErr.Add(f.opts.Close())

	f.clear()
	return multiErr.FinalError()
}
