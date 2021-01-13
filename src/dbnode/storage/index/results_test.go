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

	idxconvert "github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/test"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// only allocating a single Options to save allocs during tests
	testOpts Options
)

func init() {
	// Use size of 1 to test edge cases by default
	testOpts = optionsWithDocsArrayPool(NewOptions(), 1, 1)
}

func optionsWithDocsArrayPool(opts Options, size, capacity int) Options {
	docArrayPool := doc.NewDocumentArrayPool(doc.DocumentArrayPoolOpts{
		Options:     pool.NewObjectPoolOptions().SetSize(size),
		Capacity:    capacity,
		MaxCapacity: capacity,
	})
	docArrayPool.Init()

	return opts.SetDocumentArrayPool(docArrayPool)
}

func TestResultsInsertInvalid(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	assert.True(t, res.EnforceLimits())

	dInvalid := doc.Metadata{ID: nil}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(dInvalid),
	})
	require.Error(t, err)
	require.Equal(t, 0, size)
	require.Equal(t, 1, docsCount)

	require.Equal(t, 0, res.Size())
	require.Equal(t, 1, res.TotalDocsCount())
}

func TestResultsInsertIdempotency(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	dValid := doc.Metadata{ID: []byte("abc")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(dValid),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	require.Equal(t, 1, res.Size())
	require.Equal(t, 1, res.TotalDocsCount())

	size, docsCount, err = res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(dValid),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 2, docsCount)

	require.Equal(t, 1, res.Size())
	require.Equal(t, 2, res.TotalDocsCount())
}

func TestResultsInsertBatchOfTwo(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	d1 := doc.Metadata{ID: []byte("d1")}
	d2 := doc.Metadata{ID: []byte("d2")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(d1),
		doc.NewDocumentFromMetadata(d2),
	})
	require.NoError(t, err)
	require.Equal(t, 2, size)
	require.Equal(t, 2, docsCount)

	require.Equal(t, 2, res.Size())
	require.Equal(t, 2, res.TotalDocsCount())
}

func TestResultsFirstInsertWins(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	d1 := doc.Metadata{ID: []byte("abc")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(d1),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	require.Equal(t, 1, res.Size())
	require.Equal(t, 1, res.TotalDocsCount())

	d, ok := res.Map().Get(d1.ID)
	require.True(t, ok)

	tags := test.DocumentToTagIter(t, d)
	require.Equal(t, 0, tags.Remaining())

	d2 := doc.Metadata{
		ID: []byte("abc"),
		Fields: doc.Fields{
			doc.Field{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		}}
	size, docsCount, err = res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(d2),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 2, docsCount)

	require.Equal(t, 1, res.Size())
	require.Equal(t, 2, res.TotalDocsCount())

	d, ok = res.Map().Get([]byte("abc"))
	require.True(t, ok)

	tags = test.DocumentToTagIter(t, d)
	require.Equal(t, 0, tags.Remaining())
}

func TestResultsInsertContains(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	dValid := doc.Metadata{ID: []byte("abc")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(dValid),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	d, ok := res.Map().Get([]byte("abc"))
	require.True(t, ok)

	tags := test.DocumentToTagIter(t, d)
	require.Equal(t, 0, tags.Remaining())
}

func TestResultsInsertDoesNotCopy(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	dValid := doc.Metadata{ID: []byte("abc"), Fields: []doc.Field{
		{Name: []byte("name"), Value: []byte("value")},
	}}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(dValid),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	found := false

	// Our genny generated maps don't provide access to MapEntry directly,
	// so we iterate over the map to find the added entry. Could avoid this
	// in the future if we expose `func (m *Map) Entry(k Key) Entry {}`.
	reader := docs.NewEncodedDocumentReader()
	for _, entry := range res.Map().Iter() {
		// see if this key has the same value as the added document's ID.
		key := entry.Key()
		if !bytes.Equal(dValid.ID, key) {
			continue
		}
		found = true

		// Ensure the underlying []byte for ID/Fields is the same.
		require.True(t, xtest.ByteSlicesBackedBySameData(key, dValid.ID))
		d := entry.Value()
		m, err := docs.MetadataFromDocument(d, reader)
		require.NoError(t, err)

		tags := idxconvert.ToSeriesTags(m, idxconvert.Opts{NoClone: true})
		for _, f := range dValid.Fields {
			fName := f.Name
			fValue := f.Value

			tagsIter := tags.Duplicate()
			for tagsIter.Next() {
				tag := tagsIter.Current()
				tName := tag.Name.Bytes()
				tValue := tag.Value.Bytes()
				if !bytes.Equal(fName, tName) || !bytes.Equal(fValue, tValue) {
					continue
				}
				require.True(t, xtest.ByteSlicesBackedBySameData(fName, tName))
				require.True(t, xtest.ByteSlicesBackedBySameData(fValue, tValue))
			}
		}
	}

	require.True(t, found)
}

func TestResultsReset(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	d1 := doc.Metadata{ID: []byte("abc")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(d1),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	d, ok := res.Map().Get([]byte("abc"))
	require.True(t, ok)

	tags := test.DocumentToTagIter(t, d)
	require.Equal(t, 0, tags.Remaining())

	res.Reset(nil, QueryResultsOptions{})
	_, ok = res.Map().Get([]byte("abc"))
	require.False(t, ok)
	require.Equal(t, 0, tags.Remaining())
	require.Equal(t, 0, res.Size())
	require.Equal(t, 0, res.TotalDocsCount())
}

func TestResultsResetNamespaceClones(t *testing.T) {
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	require.Equal(t, nil, res.Namespace())
	nsID := ident.StringID("something")
	res.Reset(nsID, QueryResultsOptions{})
	nsID.Finalize()
	require.Equal(t, "something", res.Namespace().String())

	// Ensure new NS is cloned
	require.False(t,
		xtest.ByteSlicesBackedBySameData(nsID.Bytes(), res.Namespace().Bytes()))
}

func TestFinalize(t *testing.T) {
	// Create a Results and insert some data.
	res := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	d1 := doc.Metadata{ID: []byte("abc")}
	size, docsCount, err := res.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(d1),
	})
	require.NoError(t, err)
	require.Equal(t, 1, size)
	require.Equal(t, 1, docsCount)

	// Ensure the data is present.
	d, ok := res.Map().Get([]byte("abc"))
	require.True(t, ok)

	tags := test.DocumentToTagIter(t, d)
	require.Equal(t, 0, tags.Remaining())

	// Call Finalize() to reset the Results.
	res.Finalize()

	// Ensure data was removed by call to Finalize().
	_, ok = res.Map().Get([]byte("abc"))
	require.False(t, ok)
	require.Equal(t, 0, res.Size())
	require.Equal(t, 0, res.TotalDocsCount())
}
