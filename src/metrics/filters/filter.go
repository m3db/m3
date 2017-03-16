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
	"errors"
	"fmt"
	"strings"
)

var (
	errInvalidFilterPattern = errors.New("invalid filter pattern defined")
)

// LogicalOp is a logical operator
type LogicalOp string

// A list of supported logical operators
const (
	// Conjunction is logical AND
	Conjunction LogicalOp = "&&"
	// Disjunction is logical OR
	Disjunction LogicalOp = "||"
	// WildcardChar is the wildcard character
	WildcardChar = "*"
)

// Filter matches a string against certain conditions
type Filter interface {
	fmt.Stringer

	// Matches returns true if the conditions are met
	Matches(val string) bool
}

// NewFilter supports startsWith, endsWith, contains and a single wildcard
// TODO(martinm): add rest of glob matching support and negation
func NewFilter(pattern string) (Filter, error) {
	idx := strings.Index(pattern, WildcardChar)

	if idx == -1 {
		// No wildcards
		return newEqualityFilter(pattern), nil
	}

	if len(pattern) == 1 {
		// Whole thing is wildcard
		return newAllowFilter(), nil
	}

	if idx == len(pattern)-1 {
		// Wildcard at end
		return newStartsWithFilter(pattern[:len(pattern)-1]), nil
	}

	secondIdx := strings.Index(pattern[idx+1:], WildcardChar)
	if secondIdx == -1 {
		if idx == 0 {
			return newEndsWithFilter(pattern[1:]), nil
		}

		return newMultiFilter([]Filter{
			newStartsWithFilter(pattern[:idx]),
			newEndsWithFilter(pattern[idx+1:]),
		}, Conjunction), nil
	}

	if idx != 0 || secondIdx != len(pattern)-2 || secondIdx == 0 {
		return nil, errInvalidFilterPattern
	}

	return newContainsFilter(pattern[1 : len(pattern)-1]), nil
}

// allowFilter is a filter that allows all
type allowFilter struct {
}

func newAllowFilter() Filter {
	return &allowFilter{}
}

func (f *allowFilter) String() string {
	return "All"
}

func (f *allowFilter) Matches(val string) bool {
	return true
}

// equalityFilter is a filter that matches exact values
type equalityFilter struct {
	pattern string
}

func newEqualityFilter(pattern string) Filter {
	return &equalityFilter{pattern: pattern}
}

func (f *equalityFilter) String() string {
	return "Equals(\"" + f.pattern + "\")"
}

func (f *equalityFilter) Matches(val string) bool {
	return f.pattern == val
}

// startsWithFilter is a filter that performs prefix matches
type startsWithFilter struct {
	pattern string
}

func newStartsWithFilter(pattern string) Filter {
	return &startsWithFilter{pattern: pattern}
}

func (f *startsWithFilter) String() string {
	return "StartsWith(\"" + f.pattern + "\")"
}

func (f *startsWithFilter) Matches(val string) bool {
	return strings.HasPrefix(val, f.pattern)
}

// endsWithFilter is a filter that performs suffix matches
type endsWithFilter struct {
	pattern string
}

func newEndsWithFilter(pattern string) Filter {
	return &endsWithFilter{pattern: pattern}
}

func (f *endsWithFilter) String() string {
	return "EndsWith(\"" + f.pattern + "\")"
}

func (f *endsWithFilter) Matches(val string) bool {
	return strings.HasSuffix(val, f.pattern)
}

// containsFilter is a filter that performs contains matches
type containsFilter struct {
	pattern string
}

func newContainsFilter(pattern string) Filter {
	return &containsFilter{pattern: pattern}
}

func (f *containsFilter) String() string {
	return "Contains(\"" + f.pattern + "\")"
}

func (f *containsFilter) Matches(val string) bool {
	return strings.Contains(val, f.pattern)
}

// multiFilter chains multiple filters together with a logicalOp
type multiFilter struct {
	filters []Filter
	op      LogicalOp
}

func newMultiFilter(filters []Filter, op LogicalOp) Filter {
	return &multiFilter{filters: filters, op: op}
}

func (f *multiFilter) String() string {
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

func (f *multiFilter) Matches(val string) bool {
	for _, filter := range f.filters {
		match := filter.Matches(val)
		if f.op == Conjunction && match == false {
			return false
		}

		if f.op == Disjunction && match == true {
			return true
		}
	}

	return f.op == Conjunction
}
