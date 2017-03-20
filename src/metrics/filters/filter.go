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
	errInvalidFilterPattern                  = errors.New("invalid filter pattern defined")
	allowAllFilter               Filter      = allowFilter{}
	singleAnyCharFilterForwards  chainFilter = &singleAnyCharFilter{backwards: false}
	singleAnyCharFilterBackwards chainFilter = &singleAnyCharFilter{backwards: true}
)

// LogicalOp is a logical operator
type LogicalOp string

// chainSegment is the part of the pattern that the chain represents
type chainSegment int

// A list of supported logical operators
const (
	// Conjunction is logical AND
	Conjunction LogicalOp = "&&"
	// Disjunction is logical OR
	Disjunction LogicalOp = "||"

	middle chainSegment = iota
	start
	end

	wildcardChar         = '*'
	negationChar         = '!'
	singleAnyChar        = '?'
	singleRangeStartChar = '['
	singleRangeEndChar   = ']'
	rangeChar            = '-'
	invalidNestedChars   = "?["
)

// Filter matches a string against certain conditions
type Filter interface {
	fmt.Stringer

	// Matches returns true if the conditions are met
	Matches(val string) bool
}

// NewFilter supports startsWith, endsWith, contains and a single wildcard
// along with negation and glob matching support
// TODO(martinm): Add variable length glob ranges
func NewFilter(pattern string) (Filter, error) {
	if len(pattern) == 0 {
		return newEqualityFilter(pattern), nil
	}

	if pattern[0] != negationChar {
		return newWildcardFilter(pattern)
	}

	if len(pattern) == 1 {
		// only negation symbol
		return nil, errInvalidFilterPattern
	}

	filter, err := newWildcardFilter(pattern[1:])
	if err != nil {
		return nil, err
	}

	return newNegationFilter(filter), nil
}

// newWildcardFilter creates a filter that segments the pattern based
// on wildcards, creating a rangeFilter for each segment
func newWildcardFilter(pattern string) (Filter, error) {
	wIdx := strings.IndexRune(pattern, wildcardChar)

	if wIdx == -1 {
		// No wildcards
		return newRangeFilter(pattern, false, middle)
	}

	if len(pattern) == 1 {
		// Whole thing is wildcard
		return newAllowFilter(), nil
	}

	if wIdx == len(pattern)-1 {
		// Single wildcard at end
		return newRangeFilter(pattern[:len(pattern)-1], false, start)
	}

	secondWIdx := strings.IndexRune(pattern[wIdx+1:], wildcardChar)
	if secondWIdx == -1 {
		if wIdx == 0 {
			// Single wildcard at start
			return newRangeFilter(pattern[1:], true, end)
		}

		// Single wildcard in the middle
		first, err := newRangeFilter(pattern[:wIdx], false, start)
		if err != nil {
			return nil, err
		}

		second, err := newRangeFilter(pattern[wIdx+1:], true, end)
		if err != nil {
			return nil, err
		}

		return NewMultiFilter([]Filter{first, second}, Conjunction), nil
	}

	if wIdx == 0 && secondWIdx == len(pattern)-2 && len(pattern) > 2 {
		// Wildcard at beginning and end
		return newContainsFilter(pattern[1 : len(pattern)-1])
	}

	return nil, errInvalidFilterPattern
}

// newRangeFilter creates a filter that checks for ranges (? or [] or {}) and segments
// the pattern into a multiple chain filters based on ranges found
func newRangeFilter(pattern string, backwards bool, seg chainSegment) (Filter, error) {
	var filters []chainFilter
	eqIdx := -1
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == singleRangeStartChar {
			// Found '[', create an equality filter for the chars before this one if any
			// and use vals before next ']' as input for a singleRangeFilter
			if eqIdx != -1 {
				filters = append(filters, newEqualityChainFilter(pattern[eqIdx:i], backwards))
				eqIdx = -1
			}

			endIdx := strings.IndexRune(pattern[i+1:], singleRangeEndChar)
			if endIdx == -1 {
				return nil, errInvalidFilterPattern
			}

			f, err := newSingleRangeFilter(pattern[i+1:i+1+endIdx], backwards)
			if err != nil {
				return nil, errInvalidFilterPattern
			}

			filters = append(filters, f)
			i += endIdx + 1
		} else if pattern[i] == singleAnyChar {
			// Found '?', create equality filter for chars before this one if any and then
			// attach singleAnyCharFilter to chain
			if eqIdx != -1 {
				filters = append(filters, newEqualityChainFilter(pattern[eqIdx:i], backwards))
				eqIdx = -1
			}

			filters = append(filters, newSingleAnyCharFilter(backwards))
		} else if eqIdx == -1 {
			// Normal char, need to mark index to start next equality filter
			eqIdx = i
		}
	}

	if eqIdx != -1 {
		filters = append(filters, newEqualityChainFilter(pattern[eqIdx:], backwards))
	}

	return newMultiChainFilter(filters, seg, backwards), nil
}

// allowFilter is a filter that allows all
type allowFilter struct{}

func newAllowFilter() Filter                  { return allowAllFilter }
func (f allowFilter) String() string          { return "All" }
func (f allowFilter) Matches(val string) bool { return true }

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

// containsFilter is a filter that performs contains matches
type containsFilter struct {
	pattern string
}

func newContainsFilter(pattern string) (Filter, error) {
	if strings.ContainsAny(pattern, invalidNestedChars) {
		return nil, errInvalidFilterPattern
	}

	return &containsFilter{pattern: pattern}, nil
}

func (f *containsFilter) String() string {
	return "Contains(\"" + f.pattern + "\")"
}

func (f *containsFilter) Matches(val string) bool {
	return strings.Contains(val, f.pattern)
}

// negationFilter is a filter that matches the opposite of the provided filter
type negationFilter struct {
	filter Filter
}

func newNegationFilter(filter Filter) Filter {
	return &negationFilter{filter: filter}
}

func (f *negationFilter) String() string {
	return "Not(" + f.filter.String() + ")"
}

func (f *negationFilter) Matches(val string) bool {
	return !f.filter.Matches(val)
}

// multiFilter chains multiple filters together with a logicalOp
type multiFilter struct {
	filters []Filter
	op      LogicalOp
}

// NewMultiFilter returns a filter that chains multiple filters together
// using a LogicalOp
func NewMultiFilter(filters []Filter, op LogicalOp) Filter {
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
	if len(f.filters) == 0 {
		return true
	}

	for _, filter := range f.filters {
		match := filter.Matches(val)
		if f.op == Conjunction && !match {
			return false
		}

		if f.op == Disjunction && match {
			return true
		}
	}

	return f.op == Conjunction
}

// chainFilter matches an input string against certain conditions
// while returning the unmatched part of the input if there is a match
type chainFilter interface {
	fmt.Stringer

	matches(val string) (string, bool)
}

// equalityChainFilter is a filter that performs equality string matches
// from either the front or back of the string
type equalityChainFilter struct {
	pattern   string
	backwards bool
}

func newEqualityChainFilter(pattern string, backwards bool) chainFilter {
	return &equalityChainFilter{pattern: pattern, backwards: backwards}
}

func (f *equalityChainFilter) String() string {
	return "Equals(\"" + f.pattern + "\")"
}

func (f *equalityChainFilter) matches(val string) (string, bool) {
	if f.backwards && strings.HasSuffix(val, f.pattern) {
		return val[:len(val)-len(f.pattern)], true
	}

	if !f.backwards && strings.HasPrefix(val, f.pattern) {
		return val[len(f.pattern):], true
	}

	return "", false
}

// singleAnyCharFilter is a filter that allows any one char
type singleAnyCharFilter struct {
	backwards bool
}

func newSingleAnyCharFilter(backwards bool) chainFilter {
	if backwards {
		return singleAnyCharFilterBackwards
	}

	return singleAnyCharFilterForwards
}

func (f *singleAnyCharFilter) String() string { return "AnyChar" }

func (f *singleAnyCharFilter) matches(val string) (string, bool) {
	if len(val) == 0 {
		return "", false
	}

	if f.backwards {
		return val[:len(val)-1], true
	}

	return val[1:], true
}

// newSingleRangeFilter creates a filter that performs range matching
// on a single char
func newSingleRangeFilter(pattern string, backwards bool) (chainFilter, error) {
	if len(pattern) == 0 {
		return nil, errInvalidFilterPattern
	}

	negate := false
	if pattern[0] == negationChar {
		negate = true
		pattern = pattern[1:]
	}

	if len(pattern) == 3 && pattern[1] == rangeChar {
		if pattern[0] >= pattern[2] {
			return nil, errInvalidFilterPattern
		}

		return &singleRangeFilter{pattern: pattern, backwards: backwards, negate: negate}, nil
	}

	return &singleCharSetFilter{pattern: pattern, backwards: backwards, negate: negate}, nil
}

func genSingleRangeFilterStr(pattern string, negate bool) string {
	var negatePrefix, negateSuffix string
	if negate {
		negatePrefix = "Not("
		negateSuffix = ")"
	}

	return negatePrefix + "Range(\"" + pattern + "\")" + negateSuffix
}

// singleRangeFilter is a filter that performs a single character match against
// a range of chars given in a range format eg. [a-z]
type singleRangeFilter struct {
	pattern   string
	backwards bool
	negate    bool
}

func (f *singleRangeFilter) String() string {
	return genSingleRangeFilterStr(f.pattern, f.negate)
}

func (f *singleRangeFilter) matches(val string) (string, bool) {
	if len(val) == 0 {
		return "", false
	}

	idx := 0
	remainder := val[1:]
	if f.backwards {
		idx = len(val) - 1
		remainder = val[:idx]
	}

	match := val[idx] >= f.pattern[0] && val[idx] <= f.pattern[2]
	if f.negate {
		match = !match
	}

	return remainder, match
}

// singleCharSetFilter is a filter that performs a single character match against
// a set of chars given explicity eg. [abcdefg]
type singleCharSetFilter struct {
	pattern   string
	backwards bool
	negate    bool
}

func (f *singleCharSetFilter) String() string {
	return genSingleRangeFilterStr(f.pattern, f.negate)
}

func (f *singleCharSetFilter) matches(val string) (string, bool) {
	if len(val) == 0 {
		return "", false
	}

	match := false
	for i := 0; i < len(f.pattern); i++ {
		if f.backwards && val[len(val)-1] == f.pattern[i] {
			match = true
			break
		}

		if !f.backwards && val[0] == f.pattern[i] {
			match = true
			break
		}
	}

	if f.negate {
		match = !match
	}

	if f.backwards {
		return val[:len(val)-1], match
	}

	return val[1:], match
}

// multiChainFilter chains multiple chainFilters together with &&
type multiChainFilter struct {
	filters   []chainFilter
	seg       chainSegment
	backwards bool
}

// newMultiChainFilter creates a new multiChainFilter from given chainFilters
func newMultiChainFilter(filters []chainFilter, seg chainSegment, backwards bool) Filter {
	return &multiChainFilter{filters: filters, seg: seg, backwards: backwards}
}

func (f *multiChainFilter) String() string {
	separator := " then "
	var buf bytes.Buffer
	switch f.seg {
	case start:
		buf.WriteString("StartsWith(")
	case end:
		buf.WriteString("EndsWith(")
	}

	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}

	switch f.seg {
	case start, end:
		buf.WriteString(")")
	}

	return buf.String()
}

func (f *multiChainFilter) Matches(val string) bool {
	if len(f.filters) == 0 {
		return true
	}

	var match bool

	if f.backwards {
		for i := len(f.filters) - 1; i >= 0; i-- {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	} else {
		for i := 0; i < len(f.filters); i++ {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	}

	if f.seg == middle && val != "" {
		// chain was middle segment and some value was left over at end of chain
		return false
	}

	return true
}
