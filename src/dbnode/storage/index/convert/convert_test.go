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
	"testing"

	"github.com/m3db/m3db/src/dbnode/storage/index/convert"
	"github.com/m3db/m3db/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

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

func TestFromMetricInvalid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag(string(convert.ReservedFieldNameID), "value"),
	)
	_, err := convert.FromMetric(id, tags)
	assert.Error(t, err)
}

func TestFromMetricNoCloneInvalid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag(string(convert.ReservedFieldNameID), "value"),
	)
	_, err := convert.FromMetricNoClone(id, tags)
	assert.Error(t, err)
}

func TestFromMetricIteratorInvalid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag(string(convert.ReservedFieldNameID), "value"),
	)
	_, err := convert.FromMetricIter(id, ident.NewTagsIterator(tags))
	assert.Error(t, err)
}

func TestFromMetricValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromMetric(id, tags)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(d.ID))
	assert.Len(t, d.Fields, 1)
	assert.Equal(t, "bar", string(d.Fields[0].Name))
	assert.Equal(t, "baz", string(d.Fields[0].Value))
}

func TestFromMetricNoCloneValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromMetricNoClone(id, tags)
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(d.ID))
	assert.Len(t, d.Fields, 1)
	assert.Equal(t, "bar", string(d.Fields[0].Name))
	assert.Equal(t, "baz", string(d.Fields[0].Value))
}

func TestFromMetricIterValid(t *testing.T) {
	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("bar", "baz"),
	)
	d, err := convert.FromMetricIter(id, ident.NewTagsIterator(tags))
	assert.NoError(t, err)
	assert.Equal(t, "foo", string(d.ID))
	assert.Len(t, d.Fields, 1)
	assert.Equal(t, "bar", string(d.Fields[0].Name))
	assert.Equal(t, "baz", string(d.Fields[0].Value))
}

func TestToMetricValid(t *testing.T) {
	d := doc.Document{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{[]byte("bar"), []byte("baz")},
			doc.Field{[]byte("some"), []byte("others")},
		},
	}
	id, tags, err := convert.ToMetric(d, testOpts)
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

func TestToMetricInvalidID(t *testing.T) {
	d := doc.Document{
		Fields: []doc.Field{
			doc.Field{[]byte("bar"), []byte("baz")},
		},
	}
	_, _, err := convert.ToMetric(d, testOpts)
	assert.Error(t, err)
}

func TestToMetricInvalidTag(t *testing.T) {
	d := doc.Document{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{convert.ReservedFieldNameID, []byte("baz")},
		},
	}
	_, tags, err := convert.ToMetric(d, testOpts)
	assert.NoError(t, err)
	assert.False(t, tags.Next())
	assert.Error(t, tags.Err())
}

// TODO(prateek): add a test to ensure we're interacting with the Pools as expected
