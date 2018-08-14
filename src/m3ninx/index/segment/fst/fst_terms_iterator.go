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

type newFSTTermsIterOpts struct {
	opts        Options
	fst         *vellum.FST
	finalizeFST bool
}

func (o *newFSTTermsIterOpts) Close() error {
	if o.finalizeFST {
		return o.fst.Close()
	}
	return nil
}

func newFSTTermsIter(opts newFSTTermsIterOpts) *fstTermsIter {
	return &fstTermsIter{iterOpts: opts}
}

type fstTermsIter struct {
	iterOpts newFSTTermsIterOpts

	iter        *vellum.FSTIterator
	err         error
	done        bool
	initialized bool

	current      []byte
	currentValue uint64
}

var _ sgmt.OrderedBytesIterator = &fstTermsIter{}

func (f *fstTermsIter) Next() bool {
	if f.done || f.err != nil {
		return false
	}

	if f.current != nil {
		f.iterOpts.opts.BytesPool().Put(f.current)
		f.current = nil
	}

	var err error

	if !f.initialized {
		f.initialized = true
		f.iter = &vellum.FSTIterator{}
		err = f.iter.Reset(f.iterOpts.fst, minByteKey, nil, nil)
	} else {
		err = f.iter.Next()
	}

	if err != nil {
		if err != vellum.ErrIteratorDone {
			f.err = err
		}
		return false
	}

	nextBytes, nextValue := f.iter.Current()
	f.current = f.copyBytes(nextBytes)
	f.currentValue = nextValue
	return true
}

func (f *fstTermsIter) copyBytes(b []byte) []byte {
	// NB: taking a copy of the bytes to avoid referring to mmap'd memory
	l := len(b)
	bytes := f.iterOpts.opts.BytesPool().Get(l)
	bytes = bytes[:l]
	copy(bytes, b)
	return bytes
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
	return f.iterOpts.fst.Len()
}

func (f *fstTermsIter) Close() error {
	f.current = nil
	var multiErr xerrors.MultiError
	if f.iter != nil {
		multiErr = multiErr.Add(f.iter.Close())
	}
	f.iter = nil

	multiErr = multiErr.Add(f.iterOpts.Close())
	f.iterOpts = newFSTTermsIterOpts{}
	return multiErr.FinalError()
}
