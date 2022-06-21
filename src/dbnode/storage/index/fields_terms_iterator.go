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
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	pilosaroaring "github.com/m3dbx/pilosa/roaring"
)

var errUnpackBitmapFromPostingsList = errors.New("unable to unpack bitmap from postings list")

// fieldsAndTermsIteratorOpts configures the fieldsAndTermsIterator.
type fieldsAndTermsIteratorOpts struct {
	restrictByQuery *Query
	termsIterate    bool
	termsRegex      *index.CompiledRegex
	allowFn         allowFn
	fieldIterFn     newFieldIterFn
}

// nit(rob): Can we get rid of these overrides? Are they ever used?
// Might be better to make callers be explicit with their allowfn and
// newfielditerfn rather than add extra level of indirection/complexity here.
func (o fieldsAndTermsIteratorOpts) allow(f []byte) bool {
	if o.allowFn == nil {
		return true
	}
	return o.allowFn(f)
}

// nit(rob): Can we get rid of these overrides? Are they ever used?
// Might be better to make callers be explicit with their allowfn and
// newfielditerfn rather than add extra level of indirection/complexity here.
func (o fieldsAndTermsIteratorOpts) newFieldIter(r segment.Reader) (segment.FieldsPostingsListIterator, error) {
	if o.fieldIterFn == nil {
		return r.FieldsPostingsList()
	}
	return o.fieldIterFn(r)
}

type allowFn func(field []byte) bool

type newFieldIterFn func(r segment.Reader) (segment.FieldsPostingsListIterator, error)

type fieldsAndTermsIter struct {
	reader segment.Reader
	opts   fieldsAndTermsIteratorOpts

	err       error
	fieldIter segment.FieldsPostingsListIterator
	termIter  segment.TermsIterator

	current struct {
		field    []byte
		term     []byte
		postings postings.List
	}

	restrictByPostingsBitmap *pilosaroaring.Bitmap
	restrictByPostings       *roaring.ReadOnlyBitmap
}

var fieldsAndTermsIterZeroed fieldsAndTermsIter

var _ fieldsAndTermsIterator = &fieldsAndTermsIter{}

// newFieldsAndTermsIteratorFn is the lambda definition of the ctor for fieldsAndTermsIterator.
type newFieldsAndTermsIteratorFn func(
	ctx context.Context, r segment.Reader, opts fieldsAndTermsIteratorOpts,
) (fieldsAndTermsIterator, error)

func newFieldsAndTermsIterator(
	ctx context.Context,
	reader segment.Reader,
	opts fieldsAndTermsIteratorOpts,
) (fieldsAndTermsIterator, error) {
	iter := &fieldsAndTermsIter{}
	err := iter.Reset(reader, opts)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (fti *fieldsAndTermsIter) Reset(
	reader segment.Reader,
	opts fieldsAndTermsIteratorOpts,
) error {
	// Close per use items.
	if multiErr := fti.closePerUse(); multiErr.FinalError() != nil {
		return multiErr.FinalError()
	}

	// Zero state.
	*fti = fieldsAndTermsIterZeroed

	// Set per use fields.
	fti.reader = reader
	fti.opts = opts
	if reader == nil {
		return nil
	}

	fiter, err := fti.opts.newFieldIter(reader)
	if err != nil {
		return err
	}
	fti.fieldIter = fiter

	if opts.restrictByQuery == nil {
		// No need to restrict results by query.
		return nil
	}

	// If need to restrict by query, run the query on the segment first.
	searcher, err := opts.restrictByQuery.SearchQuery().Searcher()
	if err != nil {
		return err
	}

	pl, err := searcher.Search(fti.reader)
	if err != nil {
		return err
	}

	// Hold onto the postings bitmap to intersect against on a per term basis.
	if index.MigrationReadOnlyPostings() {
		// Copy into a single flat read only bitmap so that can do fast intersect.
		var (
			bitmap *pilosaroaring.Bitmap
			buff   bytes.Buffer
		)
		if b, ok := roaring.BitmapFromPostingsList(pl); ok {
			bitmap = b
		} else {
			bitmap = pilosaroaring.NewBitmap()

			iter := pl.Iterator()
			for iter.Next() {
				bitmap.DirectAdd(uint64(iter.Current()))
			}
			if err := iter.Err(); err != nil {
				return err
			}
			if err := iter.Close(); err != nil {
				return err
			}
		}

		if _, err := bitmap.WriteTo(&buff); err != nil {
			return err
		}

		fti.restrictByPostings, err = roaring.NewReadOnlyBitmap(buff.Bytes())
		if err != nil {
			return err
		}
	} else {
		var ok bool
		fti.restrictByPostingsBitmap, ok = roaring.BitmapFromPostingsList(pl)
		if !ok {
			return errUnpackBitmapFromPostingsList
		}
	}

	return nil
}

func (fti *fieldsAndTermsIter) setNextField() bool {
	fieldIter := fti.fieldIter
	if fieldIter == nil {
		return false
	}

	for fieldIter.Next() {
		field, curr := fieldIter.Current()
		if !fti.opts.allow(field) {
			continue
		}

		if index.MigrationReadOnlyPostings() && fti.restrictByPostings != nil {
			// Check term isn't part of at least some of the documents we're
			// restricted to providing results for based on intersection
			// count.
			curr, ok := roaring.ReadOnlyBitmapFromPostingsList(curr)
			if !ok {
				fti.err = fmt.Errorf("next fields postings not read only bitmap")
				return false
			}
			match := fti.restrictByPostings.IntersectsAny(curr)
			if !match {
				// No match.
				continue
			}
		} else if !index.MigrationReadOnlyPostings() && fti.restrictByPostingsBitmap != nil {
			bitmap, ok := roaring.BitmapFromPostingsList(curr)
			if !ok {
				fti.err = errUnpackBitmapFromPostingsList
				return false
			}

			// Check term isn part of at least some of the documents we're
			// restricted to providing results for based on intersection
			// count.
			// Note: IntersectionCount is significantly faster than intersecting and
			// counting results and also does not allocate.
			if n := fti.restrictByPostingsBitmap.IntersectionCount(bitmap); n < 1 {
				// No match.
				continue
			}
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
		hasNextTerm, err := fti.nextTermsIterResult()
		if err != nil {
			fti.err = err
			return false
		}
		if hasNextTerm {
			return true
		}
	}

	// i.e. need to switch to next field
	for hasNextField := fti.setNextField(); hasNextField; hasNextField = fti.setNextField() {
		// and get next term for the field
		var err error
		fti.termIter, err = fti.reader.Terms(fti.current.field)
		if err != nil {
			fti.err = err
			return false
		}

		hasNextTerm, err := fti.nextTermsIterResult()
		if err != nil {
			fti.err = err
			return false
		}
		if hasNextTerm {
			return true
		}
	}

	// Check field iterator did not encounter error.
	if err := fti.fieldIter.Err(); err != nil {
		fti.err = err
		return false
	}

	// No more fields.
	return false
}

func (fti *fieldsAndTermsIter) nextTermsIterResult() (bool, error) {
	for fti.termIter.Next() {
		fti.current.term, fti.current.postings = fti.termIter.Current()
		if index.MigrationReadOnlyPostings() {
			if fti.restrictByPostings == nil {
				// No restrictions.
				return true, nil
			}

			// Check term isn't part of at least some of the documents we're
			// restricted to providing results for based on intersection
			// count.
			curr, ok := roaring.ReadOnlyBitmapFromPostingsList(fti.current.postings)
			if !ok {
				return false, fmt.Errorf("next terms postings not read only bitmap")
			}
			match := fti.restrictByPostings.IntersectsAny(curr)
			if match {
				// Matches, this is next result.
				return true, nil
			}
		} else {
			if fti.restrictByPostingsBitmap == nil {
				// No restrictions.
				return true, nil
			}

			bitmap, ok := roaring.BitmapFromPostingsList(fti.current.postings)
			if !ok {
				return false, errUnpackBitmapFromPostingsList
			}

			// Check term isn't part of at least some of the documents we're
			// restricted to providing results for based on intersection
			// count.
			// Note: IntersectionCount is significantly faster than intersecting and
			// counting results and also does not allocate.
			if n := fti.restrictByPostingsBitmap.IntersectionCount(bitmap); n > 0 {
				// Matches, this is next result.
				return true, nil
			}
		}
	}
	if err := fti.termIter.Err(); err != nil {
		return false, err
	}
	if err := fti.termIter.Close(); err != nil {
		return false, err
	}
	// Term iterator no longer relevant, no next.
	fti.termIter = nil
	return false, nil
}

func (fti *fieldsAndTermsIter) Next() bool {
	if fti.err != nil {
		return false
	}
	// if only need to iterate fields
	if !fti.opts.termsIterate {
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

func (fti *fieldsAndTermsIter) closePerUse() xerrors.MultiError {
	var multiErr xerrors.MultiError
	if fti.fieldIter != nil {
		multiErr = multiErr.Add(fti.fieldIter.Close())
		fti.fieldIter = nil
	}
	if fti.termIter != nil {
		multiErr = multiErr.Add(fti.termIter.Close())
		fti.termIter = nil
	}
	return multiErr
}

func (fti *fieldsAndTermsIter) Close() error {
	multiErr := fti.closePerUse()
	multiErr = multiErr.Add(fti.Reset(nil, fieldsAndTermsIteratorOpts{}))
	return multiErr.FinalError()
}
