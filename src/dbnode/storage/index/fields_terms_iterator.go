// Copyright (c) 2019 Uber Technologies, Inc.
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

package index

import (
	"github.com/m3db/m3/src/m3ninx/index/segment"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// fieldsAndTermsIteratorOpts configures the fieldsAndTermsIterator.
type fieldsAndTermsIteratorOpts struct {
	iterateTerms bool
	allowFn      allowFn
	fieldIterFn  newFieldIterFn
}

func (o fieldsAndTermsIteratorOpts) allow(f []byte) bool {
	if o.allowFn == nil {
		return true
	}
	return o.allowFn(f)
}

func (o fieldsAndTermsIteratorOpts) newFieldIter(s segment.Segment) (segment.FieldsIterator, error) {
	if o.fieldIterFn == nil {
		return s.FieldsIterable().Fields()
	}
	return o.fieldIterFn(s)
}

type allowFn func(field []byte) bool

type newFieldIterFn func(s segment.Segment) (segment.FieldsIterator, error)

type fieldsAndTermsIter struct {
	seg  segment.Segment
	opts fieldsAndTermsIteratorOpts

	err       error
	fieldIter segment.FieldsIterator
	termIter  segment.TermsIterator

	current struct {
		field []byte
		term  []byte
	}
}

var (
	fieldsAndTermsIterZeroed fieldsAndTermsIter
)

var _ fieldsAndTermsIterator = &fieldsAndTermsIter{}

// newFieldsAndTermsIteratorFn is the lambda definition of the ctor for fieldsAndTermsIterator.
type newFieldsAndTermsIteratorFn func(
	s segment.Segment, opts fieldsAndTermsIteratorOpts,
) (fieldsAndTermsIterator, error)

func newFieldsAndTermsIterator(s segment.Segment, opts fieldsAndTermsIteratorOpts) (fieldsAndTermsIterator, error) {
	iter := &fieldsAndTermsIter{}
	err := iter.Reset(s, opts)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (fti *fieldsAndTermsIter) Reset(s segment.Segment, opts fieldsAndTermsIteratorOpts) error {
	*fti = fieldsAndTermsIterZeroed
	fti.seg = s
	fti.opts = opts
	if s == nil {
		return nil
	}
	fiter, err := fti.opts.newFieldIter(s)
	if err != nil {
		return err
	}
	fti.fieldIter = fiter
	return nil
}

func (fti *fieldsAndTermsIter) setNextField() bool {
	fieldIter := fti.fieldIter
	if fieldIter == nil {
		return false
	}

	for fieldIter.Next() {
		field := fieldIter.Current()
		if !fti.opts.allow(field) {
			continue
		}
		fti.current.field = field
		return true
	}

	fti.err = fieldIter.Err()
	return false
}

func (fti *fieldsAndTermsIter) setNext() bool {
	// check if current field has another term
	if fti.termIter != nil {
		if fti.termIter.Next() {
			fti.current.term, _ = fti.termIter.Current()
			return true
		}
		if err := fti.termIter.Err(); err != nil {
			fti.err = err
			return false
		}
		if err := fti.termIter.Close(); err != nil {
			fti.err = err
			return false
		}
	}

	// i.e. need to switch to next field
	hasNext := fti.setNextField()
	if !hasNext {
		return false
	}

	// and get next term for the field
	termsIter, err := fti.seg.TermsIterable().Terms(fti.current.field)
	if err != nil {
		fti.err = err
		return false
	}
	fti.termIter = termsIter

	hasNext = fti.termIter.Next()
	if !hasNext {
		if fti.fieldIter.Err(); err != nil {
			fti.err = err
			return false
		}
		fti.termIter = nil
		// i.e. no more terms for this field, should try the next one
		return fti.setNext()
	}

	fti.current.term, _ = fti.termIter.Current()
	return true
}

func (fti *fieldsAndTermsIter) Next() bool {
	if fti.err != nil {
		return false
	}
	// if only need to iterate fields
	if !fti.opts.iterateTerms {
		return fti.setNextField()
	}
	// iterating both fields and terms
	return fti.setNext()
}

func (fti *fieldsAndTermsIter) Current() (field, term []byte) {
	return fti.current.field, fti.current.term
}

func (fti *fieldsAndTermsIter) Err() error {
	return fti.err
}

func (fti *fieldsAndTermsIter) Close() error {
	var multiErr xerrors.MultiError
	if fti.fieldIter != nil {
		multiErr = multiErr.Add(fti.fieldIter.Close())
	}
	if fti.termIter != nil {
		multiErr = multiErr.Add(fti.termIter.Close())
	}
	multiErr = multiErr.Add(fti.Reset(nil, fieldsAndTermsIteratorOpts{}))
	return multiErr.FinalError()
}
