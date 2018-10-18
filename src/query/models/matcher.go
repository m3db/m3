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
	"regexp"
)

func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	default:
		return "unknown match type"
	}
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v []byte) (Matcher, error) {
	m := Matcher{
		Type:  t,
		Name:  n,
		Value: v,
	}

	if t == MatchRegexp || t == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + string(v) + ")$")
		if err != nil {
			return Matcher{}, err
		}

		m.re = re
	}

	return m, nil
}

func (m Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

// Matches returns whether the matcher matches the given string value.
func (m Matcher) Matches(s []byte) bool {
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

// ToTags converts Matchers to Tags
// NB (braskin): this only works for exact matches
func (m Matchers) ToTags(
	tagOptions TagOptions,
) (Tags, error) {
	// todo: nil is good here?
	tags := NewTags(len(m), tagOptions)
	for _, v := range m {
		if v.Type != MatchEqual {
			return Tags{}, fmt.Errorf("illegal match type, got %v, but expecting: %v", v.Type, MatchEqual)
		}

		tags = tags.AddTag(Tag{Name: v.Name, Value: v.Value}).Clone()
	}

	return tags, nil
}
