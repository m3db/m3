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

	return encoding.NewSeriesIterators(filtered, nil)
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
