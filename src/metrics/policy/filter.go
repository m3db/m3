// Copyright (c) 2016 Uber Technologies, Inc.
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

package policy

import (
	"bytes"
	"fmt"
	"sort"
)

// logicalOp is a logical operator
type logicalOp string

// A list of supported logical operators
const (
	conjunction logicalOp = "&&"
	disjunction logicalOp = "||"
)

// idFilter matches an id against certain conditions
type idFilter interface {
	fmt.Stringer

	// Matches returns true if the conditions are met
	Matches(id string) bool
}

// equalityFilter is a filter that matches metrics whose id matches pattern exactly
type equalityFilter struct {
	pattern string
}

func newEqualityFilter(pattern string) idFilter {
	return equalityFilter{pattern: pattern}
}

func (f equalityFilter) String() string {
	return fmt.Sprintf("Equals(%q)", f.pattern)
}

func (f equalityFilter) Matches(id string) bool {
	return f.pattern == id
}

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
	name   string
	filter idFilter
}

func (f tagFilter) String() string {
	return fmt.Sprintf("%s:%s", f.name, f.filter.String())
}

type tagFiltersByNameAsc []tagFilter

func (tn tagFiltersByNameAsc) Len() int           { return len(tn) }
func (tn tagFiltersByNameAsc) Swap(i, j int)      { tn[i], tn[j] = tn[j], tn[i] }
func (tn tagFiltersByNameAsc) Less(i, j int) bool { return tn[i].name < tn[j].name }

// tagsFilter contains a list of tag filters.
type tagsFilter struct {
	filters []tagFilter
	iterFn  NewSortedTagIteratorFn
}

// newTagsFilter create a new tags filter
func newTagsFilter(tagFilters map[string]string, iterFn NewSortedTagIteratorFn) idFilter {
	filters := make([]tagFilter, 0, len(tagFilters))
	for name, value := range tagFilters {
		filters = append(filters, tagFilter{
			name:   name,
			filter: newFilter(value),
		})
	}
	sort.Sort(tagFiltersByNameAsc(filters))
	return tagsFilter{
		filters: filters,
		iterFn:  iterFn,
	}
}

func (f tagsFilter) String() string {
	separator := " " + string(conjunction) + " "
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

func (f tagsFilter) Matches(id string) bool {
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
		// If the current filter tag doesn't exist, bail immediately
		if name > f.filters[currIdx].name {
			return false
		}
		// If the current filter value doesn't match provided value, bail immediately
		if !f.filters[currIdx].filter.Matches(value) {
			return false
		}
		currIdx++
	}
	return iter.Err() == nil && currIdx == len(f.filters)
}

// TODO(xichen): add support for
// * more sophisticated pattern-based filters (e.g., starts-with, any, contains, not, etc.)
// * combined filters (e.g., and filters, or filters, etc.)
func newFilter(id string) idFilter {
	return newEqualityFilter(id)
}
