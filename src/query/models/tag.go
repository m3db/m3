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
	"bytes"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strings"
)

const (
	// Separators for tags
	sep = byte(',')
	eq  = byte('=')
)

var (
	// MetricName is an internal name used to denote the name of the metric.
	// TODO: Get these from the storage
	MetricName = []byte("__name__")
)

// Tags is a list of key/value metric tag pairs
type Tags []Tag

func (t Tags) Len() int      { return len(t) }
func (t Tags) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t Tags) Less(i, j int) bool {
	return bytes.Compare(t[i].Name, t[j].Name) == -1
}

// Tag is a key/value metric tag pair
type Tag struct {
	Name, Value []byte
}

// Metric is the individual metric that gets returned from the search endpoint
type Metric struct {
	ID   string
	Tags Tags
}

// Metrics is a list of individual metrics
type Metrics []*Metric

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
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
// NB: when serialized to JSON these will be base64'd
type Matcher struct {
	Type  MatchType `json:"type"`
	Name  []byte    `json:"name"`
	Value []byte    `json:"value"`

	re *regexp.Regexp
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v []byte) (*Matcher, error) {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}
	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + string(v) + ")$")
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
func (m *Matcher) Matches(s []byte) bool {
	switch m.Type {
	case MatchEqual:
		return bytes.Equal(s, m.Value)
	case MatchNotEqual:
		return !bytes.Equal(s, m.Value)
	case MatchRegexp:
		return m.re.MatchString(string(s))
	case MatchNotRegexp:
		return !m.re.MatchString(string(s))
	}

	panic("labels.Matcher.Matches: invalid match type")
}

// Matchers is of matchers
type Matchers []*Matcher

// ToTags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func (m Matchers) ToTags() (Tags, error) {
	tags := make(Tags, len(m))
	for i, v := range m {
		if v.Type != MatchEqual {
			return nil, fmt.Errorf("illegal match type, got %v, but expecting: %v", v.Type, MatchEqual)
		}

		tags[i] = Tag{Name: v.Name, Value: v.Value}
	}

	return Normalize(tags), nil
}

// ID returns a string representation of the tags
func (t Tags) ID() string {
	var (
		idLen      = t.IDLen()
		strBuilder = strings.Builder{}
	)

	strBuilder.Grow(idLen)
	for _, tag := range t {
		strBuilder.Write(tag.Name)
		strBuilder.WriteByte(eq)
		strBuilder.Write(tag.Value)
		strBuilder.WriteByte(sep)
	}

	return strBuilder.String()
}

// IDMarshalTo writes out the ID representation
// of the tags into the provided buffer.
func (t Tags) IDMarshalTo(b []byte) []byte {
	for _, tag := range t {
		b = append(b, tag.Name...)
		b = append(b, eq)
		b = append(b, tag.Value...)
		b = append(b, sep)
	}

	return b
}

// IDLen returns the length of the ID that would be
// generated from the tags.
func (t Tags) IDLen() int {
	idLen := 2 * len(t) // account for eq and sep
	for _, tag := range t {
		idLen += len(tag.Name)
		idLen += len(tag.Value)
	}
	return idLen
}

// IDWithExcludes returns a string representation of the tags excluding some tag keys
func (t Tags) IDWithExcludes(excludeKeys ...[]byte) uint64 {
	b := make([]byte, 0, len(t))
	for _, tag := range t {
		// Always exclude the metric name by default
		if bytes.Equal(tag.Name, MetricName) {
			continue
		}

		found := false
		for _, n := range excludeKeys {
			if bytes.Equal(n, tag.Name) {
				found = true
				break
			}
		}

		// Skip the key
		if found {
			continue
		}

		b = append(b, tag.Name...)
		b = append(b, eq)
		b = append(b, tag.Value...)
		b = append(b, sep)
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

func (t Tags) tagSubset(keys [][]byte, include bool) Tags {
	tags := make(Tags, 0, len(t))
	for _, tag := range t {
		found := false
		for _, k := range keys {
			if bytes.Equal(tag.Name, k) {
				found = true
				break
			}
		}

		if found == include {
			tags = append(tags, tag)
		}
	}

	return tags
}

// TagsWithoutKeys returns only the tags which do not have the given keys
func (t Tags) TagsWithoutKeys(excludeKeys [][]byte) Tags {
	return t.tagSubset(excludeKeys, false)
}

// IDWithKeys returns a string representation of the tags only including the given keys
func (t Tags) IDWithKeys(includeKeys ...[]byte) uint64 {
	b := make([]byte, 0, len(t))
	for _, tag := range t {
		for _, k := range includeKeys {
			if bytes.Equal(tag.Name, k) {
				b = append(b, tag.Name...)
				b = append(b, eq)
				b = append(b, tag.Value...)
				b = append(b, sep)
				break
			}
		}
	}

	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// TagsWithKeys returns only the tags which have the given keys
func (t Tags) TagsWithKeys(includeKeys [][]byte) Tags {
	return t.tagSubset(includeKeys, true)
}

// WithoutName copies the tags excluding the name tag
func (t Tags) WithoutName() Tags {
	return t.TagsWithoutKeys([][]byte{MetricName})
}

// Get returns the value for the tag with the given name.
func (t Tags) Get(key []byte) ([]byte, bool) {
	for _, tag := range t {
		if bytes.Equal(tag.Name, key) {
			return tag.Value, true
		}
	}

	return nil, false
}

// Clone returns a copy of the tags
func (t Tags) Clone() Tags {
	cloned := make(Tags, len(t))
	copy(cloned, t)
	return cloned
}

// AddTag is used to add a single tag and maintain sorted order
func (t Tags) AddTag(tag Tag) Tags {
	updated := append(t, tag)
	z := Normalize(updated)
	return z
}

// ReplaceTag is used to replace a single tag's value.
// NB: if the tag does not exist, this is a noop.
func (t Tags) ReplaceTag(name, value []byte) {
	for i, tag := range t {
		if bytes.Equal(tag.Name, name) {
			t[i].Value = value
			return
		}
	}
}

// Add is used to add a list of tags and maintain sorted order
func (t Tags) Add(tags Tags) Tags {
	updated := append(t, tags...)
	return Normalize(updated)
}

// Normalize normalizes the tags by sorting them in place.
// In future, it might also ensure other things like uniqueness
func Normalize(tags Tags) Tags {
	sort.Sort(tags)
	return tags
}

// EmptyTags returns empty model tags
func EmptyTags() Tags {
	return make(Tags, 0)
}
