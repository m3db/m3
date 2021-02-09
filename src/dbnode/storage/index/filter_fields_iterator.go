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

	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

var (
	errNoFiltersSpecified = errors.New("no fields specified to filter upon")
)

func newFilterFieldsIterator(
	reader segment.Reader,
	fields AggregateFieldFilter,
) (segment.FieldsPostingsListIterator, error) {
	if len(fields) == 0 {
		return nil, errNoFiltersSpecified
	}
	fieldsIter, err := reader.FieldsPostingsList()
	if err != nil {
		return nil, err
	}
	return &filterFieldsIterator{
		reader:     reader,
		fieldsIter: fieldsIter,
		fields:     fields,
		currentIdx: -1,
	}, nil
}

type filterFieldsIterator struct {
	reader     segment.Reader
	fieldsIter segment.FieldsPostingsListIterator
	fields     AggregateFieldFilter

	err        error
	currentIdx int
}

var _ segment.FieldsPostingsListIterator = &filterFieldsIterator{}

func (f *filterFieldsIterator) Next() bool {
	if f.err != nil {
		return false
	}

	for f.fieldsIter.Next() {
		field, _ := f.fieldsIter.Current()

		found := false
		for _, f := range f.fields {
			if bytes.Equal(field, f) {
				found = true
				break
			}
		}
		if found {
			return true
		}
	}

	return false
}

func (f *filterFieldsIterator) Current() ([]byte, postings.List) {
	return f.fieldsIter.Current()
}

func (f *filterFieldsIterator) Err() error {
	return f.err
}

func (f *filterFieldsIterator) Close() error {
	return f.fieldsIter.Close()
}
