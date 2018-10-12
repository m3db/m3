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
	"hash/fnv"
	"reflect"
	"testing"
	"unsafe"

	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
)

func createTags(withName bool) Tags {
	tags := NewTags(3, nil).AddTags([]Tag{
		{Name: []byte("t1"), Value: []byte("v1")},
		{Name: []byte("t2"), Value: []byte("v2")},
	})

	if withName {
		tags = tags.SetName([]byte("v0"))
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

	idWithKeys := tags.IDWithKeys([]byte("t1"), []byte("t2"), tags.Opts.MetricName())
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

	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("t1"), tags.Opts.MetricName()})
	assert.Equal(t, []Tag{{Name: []byte("t2"), Value: []byte("v2")}}, tagsWithoutKeys.Tags)
}

func TestTagsIDLen(t *testing.T) {
	tags := NewTags(3, NewTagOptions().SetMetricName([]byte("N")))
	tags = tags.AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
	})

	tags = tags.SetName([]byte("9"))
	idLen := len("a:1,b:2,c:3,N:9,")
	assert.Equal(t, idLen, tags.IDLen())
}

func TestTagsWithExcludesCustom(t *testing.T) {
	tags := NewTags(4, nil)
	tags = tags.AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
	})

	tags.SetName([]byte("foo"))
	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("a"), []byte("c"), tags.Opts.MetricName()})
	assert.Equal(t, []Tag{{Name: []byte("b"), Value: []byte("2")}}, tagsWithoutKeys.Tags)
}

func TestAddTags(t *testing.T) {
	tags := NewTags(4, nil)

	tagToAdd := Tag{Name: []byte("x"), Value: []byte("3")}
	tags = tags.AddTag(tagToAdd)
	assert.Equal(t, []Tag{tagToAdd}, tags.Tags)

	tagsToAdd := []Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("z"), Value: []byte("4")},
	}

	tags = tags.AddTags(tagsToAdd)
	expected := []Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("x"), Value: []byte("3")},
		{Name: []byte("z"), Value: []byte("4")},
	}

	assert.Equal(t, expected, tags.Tags)
}

func TestUpdateName(t *testing.T) {
	name := []byte("!")
	tags := NewTags(1, NewTagOptions().SetMetricName(name))
	actual, found := tags.Get(name)
	assert.False(t, found)
	assert.Nil(t, actual)

	value := []byte("n")
	tags = tags.SetName(value)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value, actual)

	value2 := []byte("abc")
	tags = tags.SetName(value2)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value2, actual)
}

func TestAddOrUpdateTags(t *testing.T) {
	tags := EmptyTags().AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("z"), Value: []byte("4")},
	})

	tags = tags.AddOrUpdateTag(Tag{Name: []byte("x"), Value: []byte("!!")})
	expected := EmptyTags().AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("x"), Value: []byte("!!")},
		{Name: []byte("z"), Value: []byte("4")},
	})

	assert.Equal(t, tags, expected)
	tags = tags.AddOrUpdateTag(Tag{Name: []byte("z"), Value: []byte("?")})
	expected = EmptyTags().AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("x"), Value: []byte("!!")},
		{Name: []byte("z"), Value: []byte("?")},
	})
	assert.Equal(t, expected, tags)
}

func TestCloneTags(t *testing.T) {
	tags := createTags(true)
	cloned := tags.Clone()

	assert.Equal(t, cloned.Opts, tags.Opts)
	assert.Equal(t, cloned.Tags, tags.Tags)
	aHeader := (*reflect.SliceHeader)(unsafe.Pointer(&cloned.Tags))
	bHeader := (*reflect.SliceHeader)(unsafe.Pointer(&tags.Tags))
	assert.False(t, aHeader.Data == bHeader.Data)

	// Assert tag backing slice pointers do not match, but content is equal
	tn, tv := tags.Tags[0].Name, tags.Tags[0].Value
	cn, cv := cloned.Tags[0].Name, cloned.Tags[0].Value
	assert.True(t, bytes.Equal(tn, cn))
	assert.True(t, bytes.Equal(tv, cv))
	assert.False(t, xtest.ByteSlicesBackedBySameData(tn, cn))
	assert.False(t, xtest.ByteSlicesBackedBySameData(tv, cv))
}

func TestTagAppend(t *testing.T) {
	tagsToAdd := Tags{
		Tags: []Tag{
			{Name: []byte("x"), Value: []byte("5")},
			{Name: []byte("b"), Value: []byte("3")},
			{Name: []byte("z"), Value: []byte("1")},
			{Name: []byte("a"), Value: []byte("2")},
			{Name: []byte("c"), Value: []byte("4")},
			{Name: []byte("d"), Value: []byte("6")},
			{Name: []byte("f"), Value: []byte("7")},
		},
	}

	tags := NewTags(2, nil)
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
