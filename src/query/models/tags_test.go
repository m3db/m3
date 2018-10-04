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
)

func createTags(withName bool) Tags {
	tags := NewTags(3, nil)
	tags.Add([]Tag{
		{Name: []byte("t1"), Value: []byte("v1")},
		{Name: []byte("t2"), Value: []byte("v2")},
	})

	if withName {
		tags.SetName([]byte("v0"))
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

	b := []byte("__name__=v0,t1=v1,t2=v2,")
	h := fnv.New64a()
	h.Write(b)

	idWithKeys := tags.IDWithKeys([]byte("t1"), []byte("t2"), tags.Opts.GetMetricName())
	assert.Equal(t, h.Sum64(), idWithKeys)
}

func TestTagsWithKeys(t *testing.T) {
	tags := createTags(true)

	tagsWithKeys := tags.TagsWithKeys([][]byte{[]byte("t1")})
	assert.Equal(t, []Tag{{Name: []byte("t1"), Value: []byte("v1")}}, tagsWithKeys.Tags)
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

	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("t1"), tags.Opts.GetMetricName()})
	assert.Equal(t, []Tag{{Name: []byte("t2"), Value: []byte("v2")}}, tagsWithoutKeys.Tags)
}

func TestTagsIDLen(t *testing.T) {
	tags := NewTags(3, NewTagOptions().SetMetricName([]byte("N")))
	tags.Add([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
	})

	tags.SetName([]byte("9"))
	idLen := len("a:1,b:2,c:3,N:9,")
	assert.Equal(t, idLen, tags.IDLen())
}

func TestTagsWithExcludesCustom(t *testing.T) {
	tags := NewTags(4, nil)
	tags.Add([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
	})
	tags.SetName([]byte("foo"))

	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("a"), []byte("c"), tags.Opts.GetMetricName()})
	assert.Equal(t, []Tag{{Name: []byte("b"), Value: []byte("2")}}, tagsWithoutKeys.Tags)
}

func TestAddTags(t *testing.T) {
	tags := NewTags(4, nil)

	tagToAdd := Tag{Name: []byte("x"), Value: []byte("3")}
	tags.AddTag(tagToAdd)
	assert.Equal(t, []Tag{tagToAdd}, tags.Tags)

	tagsToAdd := []Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("z"), Value: []byte("4")},
	}

	tags.Add(tagsToAdd)
	expected := []Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("x"), Value: []byte("3")},
		{Name: []byte("z"), Value: []byte("4")},
	}

	assert.Equal(t, expected, tags.Tags)
}

func TestCloneTags(t *testing.T) {
	tags := createTags(true)
	original := createTags(true)

	cloned := tags.Clone()
	assert.Equal(t, cloned, tags)
	// mutate cloned and ensure tags is unchanged
	cloned.Tags = cloned.Tags[1:]
	assert.NotEqual(t, cloned, tags)
	assert.Equal(t, original, tags)
}

func TestTagAppend(t *testing.T) {
	tagsToAdd := []Tag{
		{Name: []byte("x"), Value: []byte("5")},
		{Name: []byte("b"), Value: []byte("3")},
		{Name: []byte("z"), Value: []byte("1")},
		{Name: []byte("a"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("4")},
		{Name: []byte("d"), Value: []byte("6")},
		{Name: []byte("f"), Value: []byte("7")},
	}

	tags := NewTags(len(tagsToAdd)-3, nil)
	tags = tags.Add(tagsToAdd)
	expected := []Tag{
		{Name: []byte("a"), Value: []byte("2")},
		{Name: []byte("b"), Value: []byte("3")},
		{Name: []byte("c"), Value: []byte("4")},
		{Name: []byte("d"), Value: []byte("6")},
		{Name: []byte("f"), Value: []byte("7")},
		{Name: []byte("x"), Value: []byte("5")},
		{Name: []byte("z"), Value: []byte("1")},
	}

	assert.Equal(t, expected, tags.Tags)
}
