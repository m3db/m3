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
package convert_test

import (
	"bytes"
	"encoding/hex"
	"testing"
	"unicode/utf8"

	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testOpts convert.Opts
)

func init() {
	// NB: allocating once to save memory in tests
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})
	testOpts.CheckedBytesPool = bytesPool
	testOpts.IdentPool = idPool
}

func TestFromSeriesIDAndTagsInvalid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag(string(convert.ReservedFieldNameID), "value"),
	)
	_, err := convert.FromSeriesIDAndTags(id, tags)
	assert.Error(t, err)
}

func TestFromSeriesIDAndTagIteratorInvalid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag(string(convert.ReservedFieldNameID), "value"),
	)
	_, err := convert.FromSeriesIDAndTagIter(id, ident.NewTagsIterator(tags))
	assert.Error(t, err)
}

func TestFromSeriesIDAndTagsValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromSeriesIDAndTags(id, tags)
	assert.NoError(t, err)
	assertContentsMatch(t, id, tags.Values(), d)
}

func TestFromSeriesIDAndTagsReuseBytesFromSeriesId(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{
			name: "tags in ID",
			id:   "bar=baz,quip=quix",
		},
		{
			name: "tags in ID with specific format",
			id:   `{bar="baz",quip="quix"}`,
		},
		{
			name: "tags in ID with specific format reverse order",
			id:   `{quip="quix",bar="baz"}`,
		},
		{
			name: "inexact tag occurrence in ID",
			id:   "quixquip_bazillion_barometers",
		},
	}
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
		ident.StringTag("quip", "quix"),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesID := ident.StringID(tt.id)
			d, err := convert.FromSeriesIDAndTags(seriesID, tags)
			assert.NoError(t, err)
			assertContentsMatch(t, seriesID, tags.Values(), d)
			for i := range d.Fields {
				assertBackedBySameData(t, d.ID, d.Fields[i].Name)
				assertBackedBySameData(t, d.ID, d.Fields[i].Value)
			}
		})
	}
}

func TestFromSeriesIDAndTagIterValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromSeriesIDAndTagIter(id, ident.NewTagsIterator(tags))
	assert.NoError(t, err)
	assertContentsMatch(t, id, tags.Values(), d)
}

func TestFromSeriesIDAndTagIterReuseBytesFromSeriesId(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{
			name: "tags in ID",
			id:   "bar=baz,quip=quix",
		},
		{
			name: "tags in ID with specific format",
			id:   `{bar="baz",quip="quix"}`,
		},
		{
			name: "tags in ID with specific format reverse order",
			id:   `{quip="quix",bar="baz"}`,
		},
		{
			name: "inexact tag occurrence in ID",
			id:   "quixquip_bazillion_barometers",
		},
	}
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
		ident.StringTag("quip", "quix"),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesID := ident.StringID(tt.id)
			d, err := convert.FromSeriesIDAndTagIter(seriesID, ident.NewTagsIterator(tags))
			assert.NoError(t, err)
			assertContentsMatch(t, seriesID, tags.Values(), d)
			for i := range d.Fields {
				assertBackedBySameData(t, d.ID, d.Fields[i].Name)
				assertBackedBySameData(t, d.ID, d.Fields[i].Value)
			}
		})
	}
}

func TestFromSeriesIDAndEncodedTags(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{
			name: "no tags in ID",
			id:   "foo",
		},
		{
			name: "tags in ID",
			id:   "bar=baz,quip=quix",
		},
		{
			name: "tags in ID with specific format",
			id:   `{bar="baz",quip="quix"}`,
		},
		{
			name: "tags in ID with specific format reverse order",
			id:   `{quip="quix",bar="baz"}`,
		},
		{
			name: "inexact tag occurrence in ID",
			id:   "quixquip_bazillion_barometers",
		},
	}
	var (
		tags = ident.NewTags(
			ident.StringTag("bar", "baz"),
			ident.StringTag("quip", "quix"),
		)
		encodedTags = toEncodedTags(t, tags)
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesID := ident.BytesID(tt.id)
			d, err := convert.FromSeriesIDAndEncodedTags(seriesID, encodedTags)
			assert.NoError(t, err)
			assertContentsMatch(t, seriesID, tags.Values(), d)
			for i := range d.Fields {
				assertBackedBySameData(t, d.ID, d.Fields[i].Name)
				assertBackedBySameData(t, d.ID, d.Fields[i].Value)
			}
		})
	}
}

func TestFromSeriesIDAndEncodedTagsInvalid(t *testing.T) {
	var (
		validEncodedTags     = []byte{117, 39, 1, 0, 3, 0, 98, 97, 114, 3, 0, 98, 97, 122}
		tagsWithReservedName = toEncodedTags(t, ident.NewTags(
			ident.StringTag(string(convert.ReservedFieldNameID), "some_value"),
		))
	)

	tests := []struct {
		name        string
		encodedTags []byte
	}{
		{
			name:        "reserved tag name",
			encodedTags: tagsWithReservedName,
		},
		{
			name:        "incomplete header",
			encodedTags: validEncodedTags[:3],
		},
		{
			name:        "incomplete tag name length",
			encodedTags: validEncodedTags[:5],
		},
		{
			name:        "incomplete tag value length",
			encodedTags: validEncodedTags[:10],
		},
		{
			name:        "invalid magic number",
			encodedTags: []byte{42, 42, 0, 0},
		},
		{
			name:        "empty tag name",
			encodedTags: []byte{117, 39, 1, 0, 0, 0, 3, 0, 98, 97, 122},
		},
	}
	seriesID := ident.BytesID("foo")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convert.FromSeriesIDAndEncodedTags(seriesID, tt.encodedTags)
			assert.Error(t, err)
		})
	}
}

func TestToSeriesValid(t *testing.T) {
	d := doc.Metadata{
		ID: []byte("foo"),
		Fields: []doc.Field{
			{Name: []byte("bar"), Value: []byte("baz")},
			{Name: []byte("some"), Value: []byte("others")},
		},
	}
	id, tags, err := convert.ToSeries(d, testOpts)
	assert.NoError(t, err)
	assert.Equal(t, 2, tags.Remaining())
	assert.Equal(t, "foo", id.String())
	assert.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "others")).Matches(tags))
}

func TestTagsFromTagsIter(t *testing.T) {
	var (
		id           = ident.StringID("foo")
		expectedTags = ident.NewTags(
			ident.StringTag("bar", "baz"),
			ident.StringTag("foo", "m3"),
		)
		tagsIter = ident.NewTagsIterator(expectedTags)
	)

	tags, err := convert.TagsFromTagsIter(id, tagsIter, testOpts.IdentPool)
	require.NoError(t, err)
	require.True(t, true, expectedTags.Equal(tags))
}

func TestTagsFromTagsIterNoPool(t *testing.T) {
	var (
		id           = ident.StringID("foo")
		expectedTags = ident.NewTags(
			ident.StringTag("bar", "baz"),
			ident.StringTag("foo", "m3"),
		)
		tagsIter = ident.NewTagsIterator(expectedTags)
	)

	tags, err := convert.TagsFromTagsIter(id, tagsIter, nil)
	require.NoError(t, err)
	require.True(t, true, expectedTags.Equal(tags))
}

func TestToSeriesInvalidID(t *testing.T) {
	d := doc.Metadata{
		Fields: []doc.Field{
			doc.Field{Name: []byte("bar"), Value: []byte("baz")},
		},
	}
	_, _, err := convert.ToSeries(d, testOpts)
	assert.Error(t, err)
}

func TestToSeriesInvalidTag(t *testing.T) {
	d := doc.Metadata{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{Name: convert.ReservedFieldNameID, Value: []byte("baz")},
		},
	}
	_, tags, err := convert.ToSeries(d, testOpts)
	assert.NoError(t, err)
	assert.False(t, tags.Next())
	assert.Error(t, tags.Err())
}

func invalidUTF8Bytes(t *testing.T) []byte {
	bytes, err := hex.DecodeString("bf")
	require.NoError(t, err)
	require.False(t, utf8.Valid(bytes))
	return bytes
}

func TestValidateSeries(t *testing.T) {
	invalidBytes := checked.NewBytes(invalidUTF8Bytes(t), nil)

	t.Run("id non-utf8", func(t *testing.T) {
		err := convert.ValidateSeries(ident.BinaryID(invalidBytes),
			ident.NewTags(ident.Tag{
				Name:  ident.StringID("bar"),
				Value: ident.StringID("baz"),
			}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid non-UTF8 ID")
	})

	t.Run("tag name reserved", func(t *testing.T) {
		reservedName := checked.NewBytes(convert.ReservedFieldNameID, nil)
		err := convert.ValidateSeries(ident.StringID("foo"),
			ident.NewTags(ident.Tag{
				Name:  ident.BinaryID(reservedName),
				Value: ident.StringID("bar"),
			}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reserved field name")
	})

	t.Run("tag name non-utf8", func(t *testing.T) {
		err := convert.ValidateSeries(ident.StringID("foo"),
			ident.NewTags(ident.Tag{
				Name:  ident.BinaryID(invalidBytes),
				Value: ident.StringID("bar"),
			}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid non-UTF8 field name")
	})

	t.Run("tag value non-utf8", func(t *testing.T) {
		err := convert.ValidateSeries(ident.StringID("foo"),
			ident.NewTags(ident.Tag{
				Name:  ident.StringID("bar"),
				Value: ident.BinaryID(invalidBytes),
			}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid non-UTF8 field value")
	})
}

// TODO(prateek): add a test to ensure we're interacting with the Pools as expected

func assertContentsMatch(t *testing.T, seriesID ident.ID, tags []ident.Tag, doc doc.Metadata) {
	assert.Equal(t, seriesID.String(), string(doc.ID))
	assert.Len(t, doc.Fields, len(tags))
	for i, f := range doc.Fields { //nolint:gocritic
		assert.Equal(t, tags[i].Name.String(), string(f.Name))
		assert.Equal(t, tags[i].Value.String(), string(f.Value))
	}
}

func assertBackedBySameData(t *testing.T, outer, inner []byte) {
	if idx := bytes.Index(outer, inner); idx != -1 {
		subslice := outer[idx : idx+len(inner)]
		assert.True(t, test.ByteSlicesBackedBySameData(subslice, inner))
	}
}

func toEncodedTags(t *testing.T, tags ident.Tags) []byte {
	pool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), nil)
	pool.Init()
	encoder := pool.Get()
	defer encoder.Finalize()

	require.NoError(t, encoder.Encode(ident.NewTagsIterator(tags)))
	data, ok := encoder.Data()
	require.True(t, ok)
	return append([]byte(nil), data.Bytes()...)
}
