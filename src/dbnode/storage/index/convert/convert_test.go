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
	"encoding/hex"
	"testing"
	"unicode/utf8"

	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"

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
	assert.Equal(t, "foo", string(d.ID))
	assert.Len(t, d.Fields, 1)
	assert.Equal(t, "bar", string(d.Fields[0].Name))
	assert.Equal(t, "baz", string(d.Fields[0].Value))
}

func TestFromSeriesIDAndTagIterValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromSeriesIDAndTagIter(id, ident.NewTagsIterator(tags))
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(d.ID))
	assert.Len(t, d.Fields, 1)
	assert.Equal(t, "bar", string(d.Fields[0].Name))
	assert.Equal(t, "baz", string(d.Fields[0].Value))
}

func TestToSeriesValid(t *testing.T) {
	d := doc.Document{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("bar"), Value: []byte("baz")},
			doc.Field{Name: []byte("some"), Value: []byte("others")},
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
	d := doc.Document{
		Fields: []doc.Field{
			doc.Field{Name: []byte("bar"), Value: []byte("baz")},
		},
	}
	_, _, err := convert.ToSeries(d, testOpts)
	assert.Error(t, err)
}

func TestToSeriesInvalidTag(t *testing.T) {
	d := doc.Document{
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
