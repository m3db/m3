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
	"sort"

	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

var (
	errNoFiltersSpecified = errors.New("no fields specified to filter upon")
)

var _ segment.FieldsPostingsListIterator = &filterFieldsIterator{}

type filterFieldsIterator struct {
	reader segment.Reader
	sorted [][]byte
	iter   segment.FieldsPostingsListIterator

	currField         []byte
	currFieldPostings postings.List
}

func newFilterFieldsIterator(
	reader segment.Reader,
	fields AggregateFieldFilter,
) (segment.FieldsPostingsListIterator, error) {
	if len(fields) == 0 {
		return nil, errNoFiltersSpecified
	}
	sorted := make([][]byte, 0, len(fields))
	for _, field := range fields {
		sorted = append(sorted, field)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})
	iter, err := reader.FieldsPostingsList()
	if err != nil {
		return nil, err
	}
	return &filterFieldsIterator{
		reader: reader,
		sorted: sorted,
		iter:   iter,
	}, nil
}

func (f *filterFieldsIterator) Next() bool {
	for f.iter.Next() && len(f.sorted) > 0 {
		f.currField, f.currFieldPostings = f.iter.Current()
		cmpResult := bytes.Compare(f.currField, f.sorted[0])
		if cmpResult < 0 {
			// This result appears before the next sorted filter.
			continue
		}
		if cmpResult > 0 {
			// Result appears after last sorted entry filtering too, no more.
			return false
		}

		f.sorted = f.sorted[1:]
		return true
	}

	return false
}

func (f *filterFieldsIterator) Current() ([]byte, postings.List) {
	return f.currField, f.currFieldPostings
}

func (f *filterFieldsIterator) Err() error {
	return f.iter.Err()
}

func (f *filterFieldsIterator) Close() error {
	return f.iter.Close()
}
