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

// Tags is a list of key/value metric tag pairs
type Tags []Tag

func (t Tags) Len() int           { return len(t) }
func (t Tags) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Tags) Less(i, j int) bool { return t[i].Name < t[j].Name }

// Tag is a key/value metric tag pair
type Tag struct {
	Name, Value string
}

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
	b := make([]byte, 0, len(t))
	for _, tag := range t {
		b = append(b, tag.Name...)
		b = append(b, eq)
		b = append(b, tag.Value...)
		b = append(b, sep)
	}

	return string(b)
}

// IDWithExcludes returns a string representation of the tags excluding some tag keys
func (t Tags) IDWithExcludes(excludeKeys ...string) uint64 {
	b := make([]byte, 0, len(t))
	for _, tag := range t {
		// Always exclude the metric name by default
		if tag.Name == MetricName {
			continue
		}

		found := false
		for _, n := range excludeKeys {
			if n == tag.Name {
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

func (t Tags) tagSubset(keys []string, include bool) Tags {
	tags := make(Tags, 0, len(t))
	for _, tag := range t {
		found := false
		for _, k := range keys {
			if tag.Name == k {
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
func (t Tags) TagsWithoutKeys(excludeKeys []string) Tags {
	return t.tagSubset(excludeKeys, false)
}

// IDWithKeys returns a string representation of the tags only including the given keys
func (t Tags) IDWithKeys(includeKeys ...string) uint64 {
	b := make([]byte, 0, len(t))
	for _, tag := range t {
		for _, k := range includeKeys {
			if tag.Name == k {
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
func (t Tags) TagsWithKeys(includeKeys []string) Tags {
	return t.tagSubset(includeKeys, true)
}

// WithoutName copies the tags excluding the name tag
func (t Tags) WithoutName() Tags {
	return t.TagsWithoutKeys([]string{MetricName})
}

// Get returns the value for the tag with the given name.
func (t Tags) Get(key string) (string, bool) {
	for _, tag := range t {
		if tag.Name == key {
			return tag.Value, true
		}
	}

	return "", false
}

// FromMap returns new sorted tags from the given map.
func FromMap(m map[string]string) Tags {
	l := make(Tags, 0, len(m))
	for k, v := range m {
		l = append(l, Tag{Name: k, Value: v})
	}

	return Normalize(l)
}

// TagMap returns a tag map of the tags.
func (t Tags) TagMap() map[string]Tag {
	m := make(map[string]Tag, len(t))
	for _, tag := range t {
		m[tag.Name] = tag
	}

	return m
}

// StringMap returns a string map of the tags.
func (t Tags) StringMap() map[string]string {
	m := make(map[string]string, len(t))
	for _, tag := range t {
		m[tag.Name] = tag.Value
	}

	return m
}

// Clone returns a copy of the tags
func (t Tags) Clone() Tags {
	cloned := make(Tags, len(t))
	copy(cloned, t)
	return cloned
}

// AddSingle is used to add a single tag and maintain sorted order
func (t Tags) AddSingle(tag Tag) Tags {
	fmt.Println("tags", t, tag)
	updated := append(t, tag)
	fmt.Println("updated", updated)

	z := Normalize(updated)
	fmt.Println("normd", z)
	return z
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
