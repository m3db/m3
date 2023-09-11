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
	"reflect"
	"testing"
	"unsafe"

	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/util/writer"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestLongTagNewIDOutOfOrderQuoted(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeQuoted)
	actual := tags.ID()
	assert.Equal(t, []byte(`{t1="v1",t2="v2",t3="v3",t4="v4"}`), actual)
}

func TestLongTagNewIDOutOfOrderGraphite(t *testing.T) {
	opts := NewTagOptions().SetIDSchemeType(TypeGraphite)
	tags := NewTags(3, opts).AddTags([]Tag{
		{Name: []byte("__g0__"), Value: []byte("v0")},
		{Name: []byte("__g10__"), Value: []byte("v10")},
		{Name: []byte("__g9__"), Value: []byte("v9")},
		{Name: []byte("__g3__"), Value: []byte("v3")},
		{Name: []byte("__g6__"), Value: []byte("v6")},
		{Name: []byte("__g11__"), Value: []byte("v11")},
		{Name: []byte("__g8__"), Value: []byte("v8")},
		{Name: []byte("__g5__"), Value: []byte("v5")},
		{Name: []byte("__g1__"), Value: []byte("v1")},
		{Name: []byte("__g7__"), Value: []byte("v7")},
		{Name: []byte("__g2__"), Value: []byte("v2")},
		{Name: []byte("__g4__"), Value: []byte("v4")},
		{Name: []byte("__g12__"), Value: []byte("v12")},
	})

	actual := tags.ID()
	assert.Equal(t, []byte("v0.v1.v2.v3.v4.v5.v6.v7.v8.v9.v10.v11.v12"), actual)
}

func TestLongTagNewIDOutOfOrderQuotedWithEscape(t *testing.T) {
	tags := testLongTagIDOutOfOrder(t, TypeQuoted)
	tags = tags.AddTag(Tag{Name: []byte(`t5""`), Value: []byte(`v"5`)})
	actual := tags.ID()
	assert.Equal(t, []byte(`{t1="v1",t2="v2",t3="v3",t4="v4",t5\"\"="v\"5"}`), actual)
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
	tags := testLongTagIDOutOfOrder(t, TypePrependMeta).
		AddTag(Tag{Name: []byte("t9"), Value: []byte(`"v1"t2"v2"`)})
	actual := tags.ID()
	expectedLength, _ := prependMetaLen(tags)
	require.Equal(t, expectedLength, len(actual))
	assert.Equal(t, []byte(`2,2,2,2,2,2,2,2,2,10!t1v1t2v2t3v3t4v4t9"v1"t2"v2"`), actual)
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

func TestAddTagsIfNotExists(t *testing.T) {
	tags := NewTags(3, nil)
	tags = tags.AddTags([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("z"), Value: []byte("4")},
	})

	tags = tags.AddTagsIfNotExists([]Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("c"), Value: []byte("3")},
	})

	expected := []Tag{
		{Name: []byte("a"), Value: []byte("1")},
		{Name: []byte("b"), Value: []byte("2")},
		{Name: []byte("c"), Value: []byte("3")},
		{Name: []byte("z"), Value: []byte("4")},
	}

	assert.Equal(t, expected, tags.Tags)
}

func TestAddTagWithoutNormalizing(t *testing.T) {
	tags := NewTags(4, nil)

	tagToAdd := Tag{Name: []byte("x"), Value: []byte("3")}
	tags = tags.AddTagWithoutNormalizing(tagToAdd)
	assert.Equal(t, []Tag{tagToAdd}, tags.Tags)

	tags = tags.AddTagWithoutNormalizing(
		Tag{Name: []byte("a"), Value: []byte("1")},
	)
	expected := []Tag{
		{Name: []byte("x"), Value: []byte("3")},
		{Name: []byte("a"), Value: []byte("1")},
	}

	assert.Equal(t, expected, tags.Tags)
	// Normalization should sort.
	tags.Normalize()
	expected[0], expected[1] = expected[1], expected[0]
	assert.Equal(t, expected, tags.Tags)
}

func TestUpdateName(t *testing.T) {
	name := []byte("!")
	tags := NewTags(1, NewTagOptions().SetMetricName(name))
	actual, found := tags.Get(name)
	assert.False(t, found)
	assert.Nil(t, actual)
	actual, found = tags.Name()
	assert.False(t, found)
	assert.Nil(t, actual)

	value := []byte("n")
	tags = tags.SetName(value)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value, actual)
	actual, found = tags.Name()
	assert.True(t, found)
	assert.Equal(t, value, actual)

	value2 := []byte("abc")
	tags = tags.SetName(value2)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value2, actual)
	actual, found = tags.Name()
	assert.True(t, found)
	assert.Equal(t, value2, actual)
}

func TestUpdateBucket(t *testing.T) {
	name := []byte("!")
	tags := NewTags(1, NewTagOptions().SetBucketName(name))
	actual, found := tags.Get(name)
	assert.False(t, found)
	assert.Nil(t, actual)
	actual, found = tags.Bucket()
	assert.False(t, found)
	assert.Nil(t, actual)

	value := []byte("n")
	tags = tags.SetBucket(value)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value, actual)
	actual, found = tags.Bucket()
	assert.True(t, found)
	assert.Equal(t, value, actual)

	value2 := []byte("abc")
	tags = tags.SetBucket(value2)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value2, actual)
	actual, found = tags.Bucket()
	assert.True(t, found)
	assert.Equal(t, value2, actual)

	value3 := []byte("")
	tags = tags.SetBucket(value3)
	actual, found = tags.Get(name)
	assert.True(t, found)
	assert.Equal(t, value3, actual)
	actual, found = tags.Bucket()
	assert.True(t, found)
	assert.Equal(t, value3, actual)
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
	assert.True(t, cloned.Equals(tags))
	assert.True(t, tags.Equals(cloned))

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

func TestTagsEquals(t *testing.T) {
	tags, other := createTags(true), createTags(true)
	assert.True(t, tags.Equals(other))

	bad := []byte("a")
	n := tags.Opts.BucketName()
	tags.Opts = tags.Opts.SetBucketName(bad)
	assert.False(t, tags.Equals(other))

	tags.Opts = tags.Opts.SetBucketName(n)
	assert.True(t, tags.Equals(other))

	n = tags.Tags[0].Name
	tags.Tags[0].Name = bad
	assert.False(t, tags.Equals(other))

	tags.Tags[0].Name = n
	assert.True(t, tags.Equals(other))

	tags = tags.AddTag(Tag{n, n})
	assert.False(t, tags.Equals(other))
}

func TestTagEquals(t *testing.T) {
	a, b, c := []byte("a"), []byte("b"), []byte("c")
	assert.True(t, Tag{a, b}.Equals(Tag{a, b}))
	assert.False(t, Tag{a, b}.Equals(Tag{a, c}))
	assert.False(t, Tag{a, b}.Equals(Tag{b, c}))
	assert.False(t, Tag{a, b}.Equals(Tag{b, b}))
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
	lengths := []int{0, 1, 2, 8, 10, 8, 100, 8, 101, 8, 110, 123456, 12345}
	l := writer.IntsLength(lengths) + 1 // account for final character
	require.Equal(t, 42, l)
	buf := make([]byte, l)
	count := writeTagLengthMeta(buf, lengths)
	require.Equal(t, 42, count)
	assert.Equal(t, []byte("0,1,2,8,10,8,100,8,101,8,110,123456,12345!"), buf)
}

func TestTagsValidateEmptyNameQuoted(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeQuoted))
	tags = tags.AddTag(Tag{Name: []byte(""), Value: []byte("bar")})
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateEmptyValueQuoted(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeQuoted))
	tags = tags.AddTag(Tag{Name: []byte("foo"), Value: []byte("")})
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateEmptyValueQuotedWithAllowTagValueEmpty(t *testing.T) {
	tags := NewTags(0, NewTagOptions().
		SetIDSchemeType(TypeQuoted).
		SetAllowTagValueEmpty(true))
	tags = tags.AddTag(Tag{Name: []byte("foo"), Value: []byte("")})
	err := tags.Validate()
	require.NoError(t, err)
}

func TestTagsValidateOutOfOrderQuoted(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeQuoted))
	tags.Tags = []Tag{
		{
			Name:  []byte("foo"),
			Value: []byte("bar"),
		},
		{
			Name:  []byte("bar"),
			Value: []byte("baz"),
		},
	}
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))

	// Test fixes after normalize.
	tags.Normalize()
	require.NoError(t, tags.Validate())
}

func TestTagsValidateDuplicateQuoted(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeQuoted))
	tags = tags.AddTag(Tag{
		Name:  []byte("foo"),
		Value: []byte("bar"),
	})
	tags = tags.AddTag(Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})
	tags = tags.AddTag(Tag{
		Name:  []byte("foo"),
		Value: []byte("qux"),
	})
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateDuplicateQuotedWithAllowTagNameDuplicates(t *testing.T) {
	tags := NewTags(0, NewTagOptions().
		SetIDSchemeType(TypeQuoted).
		SetAllowTagNameDuplicates(true))
	tags = tags.AddTag(Tag{
		Name:  []byte("foo"),
		Value: []byte("bar"),
	})
	tags = tags.AddTag(Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})
	tags = tags.AddTag(Tag{
		Name:  []byte("foo"),
		Value: []byte("qux"),
	})
	err := tags.Validate()
	require.NoError(t, err)
}

func TestTagsValidateTagNameTooLong(t *testing.T) {
	tags := NewTags(0, NewTagOptions().
		SetIDSchemeType(TypeQuoted).
		SetMaxTagLiteralLength(10))
	tags = tags.AddTag(Tag{
		Name:  []byte("name_longer_than_10_characters"),
		Value: []byte("baz"),
	})
	err := tags.Validate()
	require.EqualError(t, err, "tag name too long: index=0, length=30, maxLength=10")
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateTagValueTooLong(t *testing.T) {
	tags := NewTags(0, NewTagOptions().
		SetIDSchemeType(TypeQuoted).
		SetMaxTagLiteralLength(10))
	tags = tags.AddTag(Tag{
		Name:  []byte("bar"),
		Value: []byte("baz"),
	})
	tags = tags.AddTag(Tag{
		Name:  []byte("foo"),
		Value: []byte("value_longer_than_10_characters"),
	})
	err := tags.Validate()
	require.EqualError(t, err, "tag value too long: index=1, length=31, maxLength=10")
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateEmptyNameGraphite(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeGraphite))
	tags = tags.AddTag(Tag{Name: nil, Value: []byte("bar")})
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestTagsValidateOutOfOrderGraphite(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeGraphite))
	tags.Tags = []Tag{
		{
			Name:  graphite.TagName(10),
			Value: []byte("foo"),
		},
		{
			Name:  graphite.TagName(2),
			Value: []byte("bar"),
		},
	}
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))

	// Test fixes after normalize.
	tags.Normalize()
	require.NoError(t, tags.Validate())
}

func TestTagsValidateDuplicateGraphite(t *testing.T) {
	tags := NewTags(0, NewTagOptions().SetIDSchemeType(TypeGraphite))
	tags = tags.AddTag(Tag{
		Name:  graphite.TagName(0),
		Value: []byte("foo"),
	})
	tags = tags.AddTag(Tag{
		Name:  graphite.TagName(1),
		Value: []byte("bar"),
	})
	tags = tags.AddTag(Tag{
		Name:  graphite.TagName(1),
		Value: []byte("baz"),
	})
	err := tags.Validate()
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
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

func TestEmptyTags(t *testing.T) {
	tests := []struct {
		idType   IDSchemeType
		expected string
	}{
		{TypePrependMeta, ""},
		{TypeGraphite, ""},
		{TypeQuoted, "{}"},
	}

	for _, tt := range tests {
		tags := NewTags(0, NewTagOptions().SetIDSchemeType(tt.idType))
		id := tags.ID()
		assert.Equal(t, []byte(tt.expected), id)
	}
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
	{"_graphite", TypeGraphite},
	{"__prepend", TypePrependMeta},
	// only simple quotable tag values.
	{"___quoted", TypeQuoted},
	// only escaped tag values.
	{"__esc_qtd", typeQuotedEscaped},
}

/*
Benchmark results:

10__Tags_10__Length___legacy  5000000    236 ns/op   144 B/op  1 allocs/op
10__Tags_10__Length_graphite 10000000    174 ns/op   112 B/op  1 allocs/op
10__Tags_10__Length__prepend  3000000    537 ns/op   336 B/op  2 allocs/op
10__Tags_10__Length___quoted  3000000    404 ns/op   176 B/op  1 allocs/op
10__Tags_10__Length__esc_qtd  1000000   1324 ns/op   320 B/op  2 allocs/op

100_Tags_10__Length___legacy  1000000   2026 ns/op  1536 B/op  1 allocs/op
100_Tags_10__Length_graphite  1000000   1444 ns/op  1152 B/op  1 allocs/op
100_Tags_10__Length__prepend   300000   4601 ns/op  3584 B/op  2 allocs/op
100_Tags_10__Length___quoted   500000   3791 ns/op  1792 B/op  1 allocs/op
100_Tags_10__Length__esc_qtd   100000  12620 ns/op  3280 B/op  2 allocs/op

10__Tags_100_Length___legacy  3000000    412 ns/op  1152 B/op  1 allocs/op
10__Tags_100_Length_graphite  5000000    396 ns/op  1024 B/op  1 allocs/op
10__Tags_100_Length__prepend  2000000    803 ns/op  1312 B/op  2 allocs/op
10__Tags_100_Length___quoted  1000000   1132 ns/op  1152 B/op  1 allocs/op
10__Tags_100_Length__esc_qtd   200000  11400 ns/op  2336 B/op  2 allocs/op

100_Tags_100_Length___legacy   500000   3582 ns/op 10880 B/op  1 allocs/op
100_Tags_100_Length_graphite   500000   3183 ns/op 10240 B/op  1 allocs/op
100_Tags_100_Length__prepend   200000   6866 ns/op 14080 B/op  2 allocs/op
100_Tags_100_Length___quoted   200000  10604 ns/op 10880 B/op  1 allocs/op
100_Tags_100_Length__esc_qtd    20000  90575 ns/op 21969 B/op  2 allocs/op
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
	}
}

func TestSerializedLength(t *testing.T) {
	tag := Tag{Name: []byte("foo"), Value: []byte("bar")}
	len, escaping := serializedLength(tag)
	assert.Equal(t, 8, len)
	assert.False(t, escaping.escapeName)
	assert.False(t, escaping.escapeValue)

	tag.Name = []byte("f\ao")
	len, escaping = serializedLength(tag)
	assert.Equal(t, 9, len)
	assert.True(t, escaping.escapeName)
	assert.False(t, escaping.escapeValue)

	tag.Value = []byte(`b"ar`)
	len, escaping = serializedLength(tag)
	assert.Equal(t, 11, len)
	assert.True(t, escaping.escapeName)
	assert.True(t, escaping.escapeValue)

	tag.Name = []byte("baz")
	len, escaping = serializedLength(tag)
	assert.Equal(t, 10, len)
	assert.False(t, escaping.escapeName)
	assert.True(t, escaping.escapeValue)
}
