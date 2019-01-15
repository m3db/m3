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
	"reflect"
	"testing"
	"unsafe"

	"github.com/m3db/m3/src/query/util/writer"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
)

func testLongTagIDOutOfOrder(t *testing.T, scheme IDSchemeType) Tags {
	opts := NewTagOptions().SetIDSchemeType(scheme)
	tags := NewTags(3, opts).AddTags([]Tag{
		{Name: []byte("t1"), Value: []byte("v1")},
		{Name: []byte("t3"), Value: []byte("v3")},
		{Name: []byte("t2"), Value: []byte("v2")},
		{Name: []byte("t4"), Value: []byte("v4")},
	})

	return tags
}

func TestLongTagNewIDOutOfOrderLegacy(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeLegacy)
	actual := tags.ID()
	assert.Equal(t, tags.idLen(), len(actual))
	assert.Equal(t, []byte("t1=v1,t2=v2,t3=v3,t4=v4,"), actual)
}

func TestLongTagNewIDOutOfOrderQuoted(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeQuoted)
	actual := tags.ID()
	assert.Equal(t, []byte(`t1"v1"t2"v2"t3"v3"t4"v4"`), actual)
}

func TestHashedID(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeLegacy)
	actual := tags.HashedID()

	h := fnv.New64a()
	h.Write([]byte("t1=v1,t2=v2,t3=v3,t4=v4,"))
	expected := h.Sum64()

	assert.Equal(t, expected, actual)
}

func TestLongTagNewIDOutOfOrderQuotedWithEscape(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeQuoted)
	tags = tags.AddTag(Tag{Name: []byte(`t5""`), Value: []byte(`v"5`)})
	actual := tags.ID()
	assert.Equal(t, []byte(`t1"v1"t2"v2"t3"v3"t4"v4"t5\"\""v\"5"`), actual)
}

func TestQuotedCollisions(t *testing.T) {
	twoTags := NewTags(2, NewTagOptions().SetIDSchemeType(TypeQuoted)).
		AddTags([]Tag{
			{Name: []byte("t1"), Value: []byte("v1")},
			{Name: []byte("t2"), Value: []byte("v2")},
		})

	tagValue := NewTags(2, NewTagOptions().SetIDSchemeType(TypeQuoted)).
		AddTag(Tag{Name: []byte("t1"), Value: []byte(`"v1"t2"v2"`)})
	assert.NotEqual(t, twoTags.ID(), tagValue.ID())

	tagName := NewTags(2, NewTagOptions().SetIDSchemeType(TypeQuoted)).
		AddTag(Tag{Name: []byte(`t1"v1"t2`), Value: []byte("v2")})
	assert.NotEqual(t, twoTags.ID(), tagName.ID())

	assert.NotEqual(t, tagValue.ID(), tagName.ID())
}

func TestLongTagNewIDOutOfOrderPrefixed(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypePrependMeta)
	actual := tags.ID()
	expectedLength, _ := tags.prependMetaLen()
	assert.Equal(t, expectedLength, len(actual))
	assert.Equal(t, []byte("4444t1v1t2v2t3v3t4v4"), actual)
}

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

func TestWithoutName(t *testing.T) {
	tags := createTags(true)
	tagsWithoutName := tags.WithoutName()

	assert.Equal(t, createTags(false), tagsWithoutName)
}

func TestTagsWithKeys(t *testing.T) {
	tags := createTags(true)

	tagsWithKeys := tags.TagsWithKeys([][]byte{[]byte("t1")})
	assert.Equal(t, []Tag{{Name: []byte("t1"), Value: []byte("v1")}}, tagsWithKeys.Tags)
}

func TestTagsWithExcludes(t *testing.T) {
	tags := createTags(true)

	tagsWithoutKeys := tags.TagsWithoutKeys([][]byte{[]byte("t1"), tags.Opts.MetricName()})
	assert.Equal(t, []Tag{{Name: []byte("t2"), Value: []byte("v2")}}, tagsWithoutKeys.Tags)
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

func TestWriteTagLengthMeta(t *testing.T) {
	lengths := []int{0, 1, 2, 8, 10, 8, 100, 8, 101, 8, 110}
	l := 0
	for _, length := range lengths {
		l += writer.IntLength(length)
	}

	assert.Equal(t, 18, l)
	buf := make([]byte, l)
	count := writeTagLengthMeta(buf, lengths)
	assert.Equal(t, 18, count)
	assert.Equal(t, []byte("012810810081018110"), buf)
}

func buildTags(b *testing.B, count, length int, opts TagOptions, escape bool) Tags {
	tags := make([]Tag, count)
	for i := range tags {
		n := []byte(fmt.Sprint("t", i))
		v := make([]byte, length)
		for j := range v {
			if escape {
				v[j] = '"'
			} else {
				v[j] = 'a'
			}
		}

		tags[i] = Tag{Name: n, Value: v}
	}

	return NewTags(count, opts).AddTags(tags)
}

var tagBenchmarks = []struct {
	name                string
	tagCount, tagLength int
}{
	{"10  Tags 10  Length", 10, 10},
	{"100 Tags 10  Length", 100, 10},
	{"10  Tags 100 Length", 10, 100},
	{"100 Tags 100 Length", 100, 100},
}

const typeQuotedEscaped = IDSchemeType(100)

var tagIDSchemes = []struct {
	name   string
	scheme IDSchemeType
}{
	{"__legacy", TypeLegacy},
	{"_prepend", TypePrependMeta},
	// only simple quotable tag values.
	{"__quoted", TypeQuoted},
	// only escaped tag values.
	{"_esc_qtd", typeQuotedEscaped},
}

/*
Benchmark results:

10__Tags_10__Length__legacy-8 	10000000     242 ns/op	144 B/op
10__Tags_10__Length_prepend-8 	 5000000     388 ns/op	224 B/op
10__Tags_10__Length__quoted-8 	 5000000     385 ns/op	144 B/op
10__Tags_10__Length_esc_qtd-8 	 1000000    1385 ns/op	272 B/op

100_Tags_10__Length__legacy-8 	 1000000    2000 ns/op	1536 B/op
100_Tags_10__Length_prepend-8 	  500000    3372 ns/op	2432 B/op
100_Tags_10__Length__quoted-8 	  500000    3679 ns/op	1536 B/op
100_Tags_10__Length_esc_qtd-8 	  100000   12622 ns/op	2896 B/op

10__Tags_100_Length__legacy-8 	 3000000     451 ns/op	1152 B/op
10__Tags_100_Length_prepend-8 	 2000000     636 ns/op	1232 B/op
10__Tags_100_Length__quoted-8 	 1000000    1101 ns/op	1152 B/op
10__Tags_100_Length_esc_qtd-8 	  200000    9006 ns/op	2080 B/op

100_Tags_100_Length__legacy-8 	  300000    3731 ns/op	10880 B/op
100_Tags_100_Length_prepend-8 	  200000    5617 ns/op	11776 B/op
100_Tags_100_Length__quoted-8 	  200000   11163 ns/op	10880 B/op
100_Tags_100_Length_esc_qtd-8 	   20000  107747 ns/op	21969 B/op
*/

func BenchmarkIDs(b *testing.B) {
	opts := NewTagOptions()
	for _, bb := range tagBenchmarks {
		for _, idScheme := range tagIDSchemes {
			name := bb.name + idScheme.name
			b.Run(name, func(b *testing.B) {
				var (
					tags Tags
				)

				if idScheme.scheme == typeQuotedEscaped {
					opts = opts.SetIDSchemeType(TypeQuoted)
					tags = buildTags(b, bb.tagCount, bb.tagLength, opts, true)
				} else {
					opts = opts.SetIDSchemeType(idScheme.scheme)
					tags = buildTags(b, bb.tagCount, bb.tagLength, opts, false)
				}

				for i := 0; i < b.N; i++ {
					_ = tags.ID()
				}
			})
		}
		fmt.Println()
	}
}

func TestSerializedLength(t *testing.T) {
	tag := Tag{Name: []byte("foo"), Value: []byte("bar")}
	len, escaping := tag.serializedLength()
	assert.Equal(t, 8, len)
	assert.False(t, escaping.escapeName)
	assert.False(t, escaping.escapeValue)

	tag.Name = []byte("f\ao")
	len, escaping = tag.serializedLength()
	assert.Equal(t, 9, len)
	assert.True(t, escaping.escapeName)
	assert.False(t, escaping.escapeValue)

	tag.Value = []byte(`b"ar`)
	len, escaping = tag.serializedLength()
	assert.Equal(t, 11, len)
	assert.True(t, escaping.escapeName)
	assert.True(t, escaping.escapeValue)

	tag.Name = []byte("baz")
	len, escaping = tag.serializedLength()
	assert.Equal(t, 10, len)
	assert.False(t, escaping.escapeName)
	assert.True(t, escaping.escapeValue)
}
