// Copyright (c) 2017 Uber Technologies, Inc.
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

package filters

import (
	"bytes"
	"fmt"
	"sort"
)

// SortedTagIterator iterates over a set of tag names and values
// sorted by tag names in ascending order
type SortedTagIterator interface {
	// Next returns true if there are more tag names and values
	Next() bool

	// Current returns the current tag name and value
	Current() (string, string)

	// Err returns any errors encountered
	Err() error

	// Close closes the iterator
	Close()
}

// NewSortedTagIteratorFn creates a tag iterator given an id
type NewSortedTagIteratorFn func(id string) SortedTagIterator

// tagFilter is a filter associated with a given tag
type tagFilter struct {
	name        string
	valueFilter Filter
}

func (f *tagFilter) String() string {
	return fmt.Sprintf("%s:%s", f.name, f.valueFilter.String())
}

type tagFiltersByNameAsc []tagFilter

func (tn tagFiltersByNameAsc) Len() int           { return len(tn) }
func (tn tagFiltersByNameAsc) Swap(i, j int)      { tn[i], tn[j] = tn[j], tn[i] }
func (tn tagFiltersByNameAsc) Less(i, j int) bool { return tn[i].name < tn[j].name }

// tagsFilter contains a list of tag filters.
type tagsFilter struct {
	filters []tagFilter
	iterFn  NewSortedTagIteratorFn
	op      LogicalOp
}

// NewTagsFilter create a new tags filter
func NewTagsFilter(tagFilters map[string]string, iterFn NewSortedTagIteratorFn, op LogicalOp) (Filter, error) {
	filters := make([]tagFilter, 0, len(tagFilters))
	for name, value := range tagFilters {
		valFilter, err := NewFilter(value)
		if err != nil {
			return nil, err
		}

		filters = append(filters, tagFilter{
			name:        name,
			valueFilter: valFilter,
		})
	}
	sort.Sort(tagFiltersByNameAsc(filters))
	return &tagsFilter{
		filters: filters,
		iterFn:  iterFn,
		op:      op,
	}, nil
}

func (f *tagsFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

func (f *tagsFilter) Matches(id string) bool {
	if len(f.filters) == 0 {
		return true
	}
	iter := f.iterFn(id)
	defer iter.Close()

	currIdx := 0
	for iter.Next() && currIdx < len(f.filters) {
		name, value := iter.Current()
		if name < f.filters[currIdx].name {
			continue
		}

		if name > f.filters[currIdx].name {
			if f.op == Conjunction {
				// For AND, if the current filter tag doesn't exist, bail immediately
				return false
			}

			// Iterate filters for the OR case
			currIdx++
			for currIdx < len(f.filters) && name > f.filters[currIdx].name {
				currIdx++
			}

			if currIdx == len(f.filters) {
				// Past all filters
				return false
			}

			if name < f.filters[currIdx].name {
				continue
			}
		}

		match := f.filters[currIdx].valueFilter.Matches(value)
		if match && f.op == Disjunction {
			return true
		}

		if !match && f.op == Conjunction {
			return false
		}

		currIdx++
	}

	if iter.Err() != nil || f.op == Disjunction {
		return false
	}

	return currIdx == len(f.filters)
}
