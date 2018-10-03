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
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewMatcher(t *testing.T, mType MatchType, value string) *Matcher {
	m, err := NewMatcher(mType, []byte{}, []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMatcher(t *testing.T) {
	tests := []struct {
		matcher *Matcher
		value   string
		match   bool
	}{
		{
			matcher: mustNewMatcher(t, MatchEqual, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchEqual, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotEqual, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotEqual, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, "bar"),
			value:   "bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, "bar"),
			value:   "foo-bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchRegexp, ".*bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "bar",
			match:   false,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, "bar"),
			value:   "foo-bar",
			match:   true,
		},
		{
			matcher: mustNewMatcher(t, MatchNotRegexp, ".*bar"),
			value:   "foo-bar",
			match:   false,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.match, test.matcher.Matches([]byte(test.value)))
	}
}

func TestMatchType(t *testing.T) {
	require.Equal(t, MatchEqual.String(), "=")
}

func createTags(withName bool) Tags {
	tags := Tags{
		{Name: []byte("t1"), Value: []byte("v1")},
		{Name: []byte("t2"), Value: []byte("v2")},
	}
	if withName {
		tags = append(tags, Tag{Name: MetricName, Value: []byte("v0")})
	}
	return tags
}

func TestTagID(t *testing.T) {
	tags := createTags(false)
	assert.Equal(t, "t1=v1,t2=v2,", tags.ID())
	assert.Equal(t, tags.IDLen(), len(tags.ID()))
}

func TestTagIDMarshalTo(t *testing.T) {
	var (
		tags = createTags(false)
		b    = tags.IDMarshalTo([]byte{})
	)
	assert.Equal(t, []byte("t1=v1,t2=v2,"), b)
	assert.Equal(t, tags.IDLen(), len(b))
}

func TestWithoutName(t *testing.T) {
	tags := createTags(true)
	tagsWithoutName := tags.WithoutName()

	assert.Equal(t, createTags(false), tagsWithoutName)
}

func TestIDWithKeys(t *testing.T) {
	tags := createTags(true)

	b := []byte("t1=v1,t2=v2,__name__=v0,")
	h := fnv.New64a()
	h.Write(b)

	idWithKeys := tags.IDWithKeys([]byte("t1"), []byte("t2"), MetricName)
	assert.Equal(t, h.Sum64(), idWithKeys)
}

func TestTagsWithKeys(t *testing.T) {
	tags := createTags(true)

	tagsWithKeys := tags.TagsWithKeys([][]byte{[]byte("t1")})
	assert.Equal(t, Tags{{Name: []byte("t1"), Value: []byte("v1")}}, tagsWithKeys)
}

func TestIDWithExcludes(t *testing.T) {
	tags := createTags(true)

	b := []byte("t2=v2,")
	h := fnv.New64a()
	h.Write(b)

	idWithExcludes := tags.IDWithExcludes([]byte("t1"))
	assert.Equal(t, h.Sum64(), idWithExcludes)
}

func TestTagsWithExcludes(t *testing.T) {
	tags := createTags(true)

	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("t1"), MetricName})
	assert.Equal(t, Tags{{Name: []byte("t2"), Value: []byte("v2")}}, tagsWithoutKeys)
}

func TestTagsWithExcludesCustom(t *testing.T) {
	tags := Tags{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
		{Name: MetricName, Value: []byte("foo")},
	}
	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("a"), []byte("c"), MetricName})
	assert.Equal(t, Tags{{Name: []byte("b"), Value: []byte("2")}}, tagsWithoutKeys)
}

func TestAddTags(t *testing.T) {
	tags := make(Tags, 0, 4)

	tagToAdd := Tag{Name: []byte("x"), Value: []byte("3")}
	tags = tags.AddTag(tagToAdd)
	assert.Equal(t, Tags{tagToAdd}, tags)

	tagsToAdd := Tags{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("z"), Value: []byte("4")},
	}
	tags = tags.Add(tagsToAdd)

	expected := Tags{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("x"), Value: []byte("3")},
		{Name: []byte("z"), Value: []byte("4")},
	}
	assert.Equal(t, expected, tags)
}

func TestCloneTags(t *testing.T) {
	tags := createTags(true)
	original := createTags(true)

	cloned := tags.Clone()
	assert.Equal(t, cloned, tags)
	// mutate cloned and ensure tags is unchanged
	cloned = cloned[1:]
	assert.NotEqual(t, cloned, tags)
	assert.Equal(t, original, tags)
}
