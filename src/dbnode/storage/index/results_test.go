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

package index

import (
	"bytes"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

var (
	// only allocating a single Options to save allocs during tests
	testOpts Options
)

func init() {
	testOpts = NewOptions()
}

func TestResultsInsertInvalid(t *testing.T) {
	res := NewResults(testOpts)
	dInvalid := doc.Document{ID: nil}
	added, size, err := res.AddDocument(dInvalid)
	require.Error(t, err)
	require.False(t, added)
	require.Equal(t, 0, size)
}

func TestResultsInsertIdempotency(t *testing.T) {
	res := NewResults(testOpts)
	dValid := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(dValid)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	added, size, err = res.AddDocument(dValid)
	require.NoError(t, err)
	require.False(t, added)
	require.Equal(t, 1, size)
}

func TestResultsFirstInsertWins(t *testing.T) {
	res := NewResults(testOpts)
	d1 := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(d1)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))

	d2 := doc.Document{ID: []byte("abc"),
		Fields: doc.Fields{
			doc.Field{Name: []byte("foo"), Value: []byte("bar")},
		}}
	added, size, err = res.AddDocument(d2)
	require.NoError(t, err)
	require.False(t, added)
	require.Equal(t, 1, size)

	tags, ok = res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))
}

func TestResultsInsertContains(t *testing.T) {
	res := NewResults(testOpts)
	dValid := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(dValid)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))
}

func TestResultsInsertCopies(t *testing.T) {
	res := NewResults(testOpts)
	dValid := doc.Document{ID: []byte("abc"), Fields: []doc.Field{
		doc.Field{Name: []byte("name"), Value: []byte("value")},
	}}
	added, size, err := res.AddDocument(dValid)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	found := false
	// our genny generated maps don't provide access to MapEntry directly,
	// so we iterate over the map to find the added entry. Could avoid this
	// in the future if we expose `func (m *Map) Entry(k Key) Entry {}`
	for _, entry := range res.Map().Iter() {
		// see if this key has the same value as the added document's ID.
		key := entry.Key().Bytes()
		if !bytes.Equal(dValid.ID, key) {
			continue
		}
		found = true
		// ensure the underlying []byte for ID/Fields is at a different address
		// than the original.
		require.False(t, xtest.ByteSlicesBackedBySameData(key, dValid.ID))
		tags := entry.Value().Values()
		for _, f := range dValid.Fields {
			fName := f.Name
			fValue := f.Value
			for _, tag := range tags {
				tName := tag.Name.Bytes()
				tValue := tag.Value.Bytes()
				if !bytes.Equal(fName, tName) || !bytes.Equal(fValue, tValue) {
					continue
				}
				require.False(t, xtest.ByteSlicesBackedBySameData(fName, tName))
				require.False(t, xtest.ByteSlicesBackedBySameData(fValue, tValue))
			}
		}
	}

	require.True(t, found)
}

func TestResultsInsertIDAndTagsInvalid(t *testing.T) {
	res := NewResults(testOpts)
	added, size, err := res.AddIDAndTags(ident.BytesID(nil), ident.Tags{})
	require.Error(t, err)
	require.False(t, added)
	require.Equal(t, 0, size)
}

func TestResultsInsertIDAndTagsIdempotency(t *testing.T) {
	res := NewResults(testOpts)
	dValid := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddIDAndTags(ident.BytesID(dValid.ID), tagsFromFields(dValid.Fields))
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	added, size, err = res.AddIDAndTags(ident.BytesID(dValid.ID), tagsFromFields(dValid.Fields))
	require.NoError(t, err)
	require.False(t, added)
	require.Equal(t, 1, size)
}

func TestResultsIDAndTagsFirstInsertWins(t *testing.T) {
	res := NewResults(testOpts)
	d1 := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddIDAndTags(ident.BytesID(d1.ID), tagsFromFields(d1.Fields))
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))

	d2 := doc.Document{ID: []byte("abc"),
		Fields: doc.Fields{
			doc.Field{Name: []byte("foo"), Value: []byte("bar")},
		}}
	added, size, err = res.AddIDAndTags(ident.BytesID(d2.ID), tagsFromFields(d2.Fields))
	require.NoError(t, err)
	require.False(t, added)
	require.Equal(t, 1, size)

	tags, ok = res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))
}

func TestResultsReset(t *testing.T) {
	res := NewResults(testOpts)
	d1 := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(d1)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))

	res.Reset(nil)
	_, ok = res.Map().Get(ident.StringID("abc"))
	require.False(t, ok)
	require.Equal(t, 0, len(tags.Values()))
	require.Equal(t, 0, res.Size())
}

func TestResultsResetNamespaceClones(t *testing.T) {
	res := NewResults(testOpts)
	require.Equal(t, nil, res.Namespace())
	nsID := ident.StringID("something")
	res.Reset(nsID)
	nsID.Finalize()
	require.Equal(t, "something", res.Namespace().String())
}

func tagsFromFields(fields []doc.Field) ident.Tags {
	tags := ident.NewTags()
	for _, field := range fields {
		tags.Append(ident.Tag{
			Name:  ident.BytesID(field.Name),
			Value: ident.BytesID(field.Value),
		})
	}
	return tags
}

func TestFinalize(t *testing.T) {
	// Create a Results and insert some data.
	res := NewResults(testOpts)
	d1 := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(d1)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	// Ensure the data is present.
	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))

	// Call Finalize() to reset the Results.
	res.Finalize()

	// Ensure data was removed by call to Finalize().
	tags, ok = res.Map().Get(ident.StringID("abc"))
	require.False(t, ok)
	require.Equal(t, 0, len(tags.Values()))
	require.Equal(t, 0, res.Size())

	for _, entry := range res.Map().Iter() {
		id, _ := entry.Key(), entry.Value()
		require.False(t, id.IsNoFinalize())
		// TODO(rartoul): Could verify tags are NoFinalize() as well if
		// they had that method.
	}
}

func TestNoFinalize(t *testing.T) {
	// Create a Results and insert some data.
	res := NewResults(testOpts)
	d1 := doc.Document{ID: []byte("abc")}
	added, size, err := res.AddDocument(d1)
	require.NoError(t, err)
	require.True(t, added)
	require.Equal(t, 1, size)

	// Ensure the data is present.
	tags, ok := res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))

	// Call to NoFinalize indicates that subsequent call
	// to finalize should be a no-op.
	res.NoFinalize()
	res.Finalize()

	// Ensure data was not removed by call to Finalize().
	tags, ok = res.Map().Get(ident.StringID("abc"))
	require.True(t, ok)
	require.Equal(t, 0, len(tags.Values()))
	require.Equal(t, 1, res.Size())

	for _, entry := range res.Map().Iter() {
		id, _ := entry.Key(), entry.Value()
		require.True(t, id.IsNoFinalize())
		// TODO(rartoul): Could verify tags are NoFinalize() as well if
		// they had that method.
	}
}
