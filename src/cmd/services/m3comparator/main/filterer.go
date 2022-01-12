// Copyright (c) 2020 Uber Technologies, Inc.
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

package main

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
)

func filter(series encoding.SeriesIterators, tagMatchers models.Matchers) encoding.SeriesIterators {
	var filtered []encoding.SeriesIterator

	for _, iter := range series.Iters() {
		if matchesAll(tagMatchers, iter.Tags()) {
			filtered = append(filtered, iter)
		}
	}

	return encoding.NewSeriesIterators(filtered)
}

func matchesAll(tagMatchers models.Matchers, tagsIter ident.TagIterator) bool {
	for _, tagMatcher := range tagMatchers {
		if !isIgnored(tagMatcher) && !matchesOne(tagMatcher, tagsIter) {
			return false
		}
	}

	return true
}

func matchesOne(tagMatcher models.Matcher, tagsIter ident.TagIterator) bool {
	tag := findTag(tagMatcher.Name, tagsIter)

	return matches(tagMatcher, tag)
}

func isIgnored(tagMatcher models.Matcher) bool {
	return tagMatcher.String() == `role="remote"` // this matcher gets injected by Prometheus
}

func findTag(name []byte, tagsIter ident.TagIterator) ident.Tag {
	tagsIter = tagsIter.Duplicate()
	defer tagsIter.Close()

	for tagsIter.Next() {
		tag := tagsIter.Current()
		if bytes.Equal(tag.Name.Bytes(), name) {
			return tag
		}
	}

	return ident.StringTag("", "")
}

func matches(tagMatcher models.Matcher, tag ident.Tag) bool {
	var (
		name   = tag.Name.Bytes()
		value  = tag.Value.Bytes()
		invert = false
	)

	switch tagMatcher.Type {

	case models.MatchNotEqual:
		invert = true
		fallthrough

	case models.MatchEqual:
		return bytes.Equal(tagMatcher.Value, value) != invert

	case models.MatchNotRegexp:
		invert = true
		fallthrough

	case models.MatchRegexp:
		m, _ := regexp.Match(fmt.Sprintf("^%s$", tagMatcher.Value), value)
		return m != invert

	case models.MatchNotField:
		invert = true
		fallthrough

	case models.MatchField:
		return bytes.Equal(tagMatcher.Name, name) != invert

	case models.MatchAll:
		return true
	}

	return false
}
