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

package models

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
)

const (
	// MetricName is an internal name used to denote the name of the metric.
	// TODO: Get these from the storage
	MetricName = "__name__"

	// Separators for tags
	sep = byte(',')
	eq  = byte('=')
)

// Tags is a key/value map of metric tags.
type Tags map[string]string

// Metric is the individual metric that gets returned from the search endpoint
type Metric struct {
	Namespace string
	ID        string
	Tags      Tags
}

// Metrics is a list of individual metrics
type Metrics []*Metric

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual     MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	typeToStr := map[MatchType]string{
		MatchEqual:     "=",
		MatchNotEqual:  "!=",
		MatchRegexp:    "=~",
		MatchNotRegexp: "!~",
	}
	if str, ok := typeToStr[m]; ok {
		return str
	}
	panic("unknown match type")
}

// Matcher models the matching of a label.
type Matcher struct {
	Type  MatchType `json:"type"`
	Name  string    `json:"name"`
	Value string    `json:"value"`

	re *regexp.Regexp
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + v + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

func (m *Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// Matches returns whether the matcher matches the given string value.
func (m *Matcher) Matches(s string) bool {
	switch m.Type {
	case MatchEqual:
		return s == m.Value
	case MatchNotEqual:
		return s != m.Value
	case MatchRegexp:
		return m.re.MatchString(s)
	case MatchNotRegexp:
		return !m.re.MatchString(s)
	}
	panic("labels.Matcher.Matches: invalid match type")
}

// Matchers is of matchers
type Matchers []*Matcher

// ToTags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func (m Matchers) ToTags() (Tags, error) {
	tags := make(Tags, len(m))
	for _, v := range m {
		if v.Type != MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", v.Type, MatchEqual)
		}
		tags[v.Name] = v.Value
	}

	return tags, nil
}

// ID returns a string representation of the tags
func (t Tags) ID() string {
	sortedKeys, bufLength := t.sortKeys()
	b := make([]byte, 0, bufLength)
	for _, k := range sortedKeys {
		b = append(b, k...)
		b = append(b, eq)
		b = append(b, t[k]...)
		b = append(b, sep)
	}

	return string(b)
}

// IDWithExcludes returns a string representation of the tags excluding some tag keys
func (t Tags) IDWithExcludes(excludeKeys ...string) uint64 {
	sortedKeys, bufLength := t.sortKeys()
	b := make([]byte, 0, bufLength)
	for _, k := range sortedKeys {
		// Always exclude the metric name by default
		if k == MetricName {
			continue
		}

		found := false
		for _, n := range excludeKeys {
			if n == k {
				found = true
				break
			}
		}

		// Skip the key
		if found {
			continue
		}

		b = append(b, k...)
		b = append(b, eq)
		b = append(b, t[k]...)
		b = append(b, sep)
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// IDWithKeys returns a string representation of the tags only including the given keys
func (t Tags) IDWithKeys(includeKeys ...string) uint64 {
	b := make([]byte, 0, len(t))
	for _, k := range includeKeys {
		v, ok := t[k]
		if !ok {
			continue
		}

		b = append(b, k...)
		b = append(b, eq)
		b = append(b, v...)
		b = append(b, sep)
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

func (t Tags) sortKeys() ([]string, int) {
	length := 0
	keys := make([]string, 0, len(t))
	for k := range t {
		length += len(k) + len(t[k]) + 2
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, length
}
