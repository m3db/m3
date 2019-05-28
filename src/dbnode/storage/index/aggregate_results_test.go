// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func genDoc(strs ...string) doc.Document {
	if len(strs)%2 != 0 {
		panic("invalid test setup; need even str length")
	}

	fields := make([]doc.Field, len(strs)/2)
	for i := range fields {
		fields[i] = doc.Field{
			Name:  []byte(strs[i*2]),
			Value: []byte(strs[i*2+1]),
		}
	}

	return doc.Document{Fields: fields}
}

func TestAggResultsInsertInvalid(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	dInvalid := doc.Document{Fields: []doc.Field{{}}}
	size, err := res.AddDocuments([]doc.Document{dInvalid})
	require.Error(t, err)
	require.Equal(t, 0, size)

	dInvalid = genDoc("", "foo")
	size, err = res.AddDocuments([]doc.Document{dInvalid})
	require.Error(t, err)
	require.Equal(t, 0, size)

	dInvalid = genDoc("foo", "")
	size, err = res.AddDocuments([]doc.Document{dInvalid})
	require.Error(t, err)
	require.Equal(t, 0, size)
}

func TestAggResultsTermOnlyInsert(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{
		Type: AggregateTagNames,
	}, testOpts)
	dInvalid := doc.Document{Fields: []doc.Field{{}}}
	size, err := res.AddDocuments([]doc.Document{dInvalid})
	require.Error(t, err)
	require.Equal(t, 0, size)

	dInvalid = genDoc("", "foo")
	size, err = res.AddDocuments([]doc.Document{dInvalid})
	require.Error(t, err)
	require.Equal(t, 0, size)

	valid := genDoc("foo", "")
	size, err = res.AddDocuments([]doc.Document{valid})
	require.NoError(t, err)
	require.Equal(t, 1, size)
}

func testAggResultsInsertIdempotency(t *testing.T, res AggregateResults) {
	dValid := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{dValid})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	size, err = res.AddDocuments([]doc.Document{dValid})
	require.NoError(t, err)
	require.Equal(t, 1, size)
}

func TestAggResultsInsertIdempotency(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	testAggResultsInsertIdempotency(t, res)
}

func TestAggResultsTermOnlyInsertIdempotency(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{
		Type: AggregateTagNames,
	}, testOpts)
	testAggResultsInsertIdempotency(t, res)
}

func TestInvalidAggregateType(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{
		Type: 100,
	}, testOpts)
	dValid := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{dValid})
	require.Error(t, err)
	require.Equal(t, 0, size)
}

func TestAggResultsSameName(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	d1 := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{d1})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	rMap := res.Map()
	aggVals, ok := rMap.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 1, aggVals.Size())
	assert.True(t, aggVals.Map().Contains(ident.StringID("bar")))

	d2 := genDoc("foo", "biz")
	size, err = res.AddDocuments([]doc.Document{d2})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	aggVals, ok = rMap.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 2, aggVals.Size())
	assert.True(t, aggVals.Map().Contains(ident.StringID("bar")))
	assert.True(t, aggVals.Map().Contains(ident.StringID("biz")))
}

func TestAggResultsTermOnlySameName(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{
		Type: AggregateTagNames,
	}, testOpts)
	d1 := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{d1})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	rMap := res.Map()
	aggVals, ok := rMap.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 0, aggVals.Size())

	d2 := genDoc("foo", "biz")
	size, err = res.AddDocuments([]doc.Document{d2})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	aggVals, ok = rMap.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 0, aggVals.Size())
}

func assertContains(t *testing.T,
	ex map[string][]string, ac *AggregateResultsMap) {
	require.Equal(t, len(ex), ac.Len())
	for k, v := range ex {
		aggVals, ok := ac.Get(ident.StringID(k))
		require.True(t, ok)
		require.Equal(t, len(v), aggVals.Size())
		for _, actual := range v {
			require.True(t, aggVals.Map().Contains(ident.StringID(actual)))
		}
	}
}

func addMultipleDocuments(t *testing.T, res AggregateResults) int {
	_, err := res.AddDocuments([]doc.Document{
		genDoc("foo", "bar"),
		genDoc("fizz", "bar"),
		genDoc("buzz", "bar"),
	})
	require.NoError(t, err)

	_, err = res.AddDocuments([]doc.Document{
		genDoc("foo", "biz"),
		genDoc("fizz", "bar"),
	})
	require.NoError(t, err)

	size, err := res.AddDocuments([]doc.Document{
		genDoc("foo", "baz", "buzz", "bag", "qux", "qaz"),
	})

	require.NoError(t, err)
	return size
}

func expectedTermsOnly(ex map[string][]string) map[string][]string {
	m := make(map[string][]string, len(ex))
	for k := range ex {
		m[k] = []string{}
	}

	return m
}

func toFilter(strs ...string) AggregateFieldFilter {
	b := make([][]byte, len(strs))
	for i, s := range strs {
		b[i] = []byte(s)
	}

	return AggregateFieldFilter(b)
}

var mergeTests = []struct {
	name     string
	opts     AggregateResultsOptions
	expected map[string][]string
}{
	{
		name: "no limit no filter",
		opts: AggregateResultsOptions{},
		expected: map[string][]string{
			"foo":  []string{"bar", "biz", "baz"},
			"fizz": []string{"bar"},
			"buzz": []string{"bar", "bag"},
			"qux":  []string{"qaz"},
		},
	},
	{
		name: "with limit no filter",
		opts: AggregateResultsOptions{SizeLimit: 2},
		expected: map[string][]string{
			"foo":  []string{"bar", "biz", "baz"},
			"fizz": []string{"bar"},
		},
	},
	{
		name: "no limit empty filter",
		opts: AggregateResultsOptions{FieldFilter: toFilter()},
		expected: map[string][]string{
			"foo":  []string{"bar", "biz", "baz"},
			"fizz": []string{"bar"},
			"buzz": []string{"bar", "bag"},
			"qux":  []string{"qaz"},
		},
	},
	{
		name:     "no limit matchless filter",
		opts:     AggregateResultsOptions{FieldFilter: toFilter("zig")},
		expected: map[string][]string{},
	},
	{
		name: "empty limit with filter",
		opts: AggregateResultsOptions{FieldFilter: toFilter("buzz")},
		expected: map[string][]string{
			"buzz": []string{"bar", "bag"},
		},
	},
	{
		name: "with limit with filter",
		opts: AggregateResultsOptions{
			SizeLimit: 2, FieldFilter: toFilter("buzz", "qux", "fizz")},
		expected: map[string][]string{
			"fizz": []string{"bar"},
			"buzz": []string{"bar", "bag"},
		},
	},
}

func TestAggResultsMerge(t *testing.T) {
	for _, tt := range mergeTests {
		t.Run(tt.name, func(t *testing.T) {
			res := NewAggregateResults(nil, tt.opts, testOpts)
			size := addMultipleDocuments(t, res)

			require.Equal(t, len(tt.expected), size)
			assertContains(t, tt.expected, res.Map())
		})

		t.Run(tt.name+" name only", func(t *testing.T) {
			tt.opts.Type = AggregateTagNames
			res := NewAggregateResults(nil, tt.opts, testOpts)
			size := addMultipleDocuments(t, res)

			require.Equal(t, len(tt.expected), size)
			assertContains(t, expectedTermsOnly(tt.expected), res.Map())
		})
	}
}

func TestAggResultsInsertCopies(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	dValid := genDoc("foo", "bar")
	name := dValid.Fields[0].Name
	value := dValid.Fields[0].Value
	size, err := res.AddDocuments([]doc.Document{dValid})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	found := false

	// our genny generated maps don't provide access to MapEntry directly,
	// so we iterate over the map to find the added entry. Could avoid this
	// in the future if we expose `func (m *Map) Entry(k Key) Entry {}`
	for _, entry := range res.Map().Iter() {
		// see if this key has the same value as the added document's ID.
		n := entry.Key().Bytes()
		if !bytes.Equal(name, n) {
			continue
		}
		// ensure the underlying []byte for ID/Fields is at a different address
		// than the original.
		require.False(t, xtest.ByteSlicesBackedBySameData(n, name))
		v := entry.Value()
		for _, f := range v.Map().Iter() {
			v := f.Key().Bytes()
			if !bytes.Equal(value, v) {
				continue
			}

			found = true
			// ensure the underlying []byte for ID/Fields is at a different address
			// than the original.
			require.False(t, xtest.ByteSlicesBackedBySameData(v, value))
		}
	}

	require.True(t, found)
}

func TestAggResultsNameOnlyInsertCopies(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{
		Type: AggregateTagNames,
	}, testOpts)
	dValid := genDoc("foo", "bar")
	name := dValid.Fields[0].Name
	size, err := res.AddDocuments([]doc.Document{dValid})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	found := false
	// our genny generated maps don't provide access to MapEntry directly,
	// so we iterate over the map to find the added entry. Could avoid this
	// in the future if we expose `func (m *Map) Entry(k Key) Entry {}`
	for _, entry := range res.Map().Iter() {
		// see if this key has the same value as the added document's ID.
		n := entry.Key().Bytes()
		if !bytes.Equal(name, n) {
			continue
		}

		// ensure the underlying []byte for ID/Fields is at a different address
		// than the original.
		require.False(t, xtest.ByteSlicesBackedBySameData(n, name))
		found = true
		v := entry.Value()
		require.NotNil(t, v.Map())
		require.Equal(t, 0, v.Size())
	}

	require.True(t, found)
}

func TestAggResultsReset(t *testing.T) {
	res := NewAggregateResults(ident.StringID("qux"),
		AggregateResultsOptions{}, testOpts)
	d1 := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{d1})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	aggVals, ok := res.Map().Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 1, aggVals.Size())

	// Check result options correct.
	aggResults, ok := res.(*aggregatedResults)
	require.True(t, ok)
	require.Equal(t, 0, aggResults.aggregateOpts.SizeLimit)
	require.Equal(t, ident.StringID("qux"), aggResults.nsID)

	newID := ident.StringID("qaz")
	res.Reset(newID, AggregateResultsOptions{SizeLimit: 100})
	_, ok = res.Map().Get(ident.StringID("foo"))
	require.False(t, ok)
	require.Equal(t, 0, aggVals.Size())
	require.Equal(t, 0, res.Size())

	// Check result options change.
	aggResults, ok = res.(*aggregatedResults)
	require.True(t, ok)
	require.Equal(t, 100, aggResults.aggregateOpts.SizeLimit)
	fmt.Println(aggResults.nsID.String())
	require.Equal(t, newID.Bytes(), aggResults.nsID.Bytes())

	// Ensure new NS is cloned
	require.False(t,
		xtest.ByteSlicesBackedBySameData(newID.Bytes(), aggResults.nsID.Bytes()))
}

func TestAggResultsResetNamespaceClones(t *testing.T) {
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	require.Equal(t, nil, res.Namespace())
	nsID := ident.StringID("something")
	res.Reset(nsID, AggregateResultsOptions{})
	nsID.Finalize()
	require.Equal(t, nsID.Bytes(), res.Namespace().Bytes())

	// Ensure new NS is cloned
	require.False(t,
		xtest.ByteSlicesBackedBySameData(nsID.Bytes(), res.Namespace().Bytes()))
}

func TestAggResultFinalize(t *testing.T) {
	// Create a Results and insert some data.
	res := NewAggregateResults(nil, AggregateResultsOptions{}, testOpts)
	d1 := genDoc("foo", "bar")
	size, err := res.AddDocuments([]doc.Document{d1})
	require.NoError(t, err)
	require.Equal(t, 1, size)

	// Ensure the data is present.
	rMap := res.Map()
	aggVals, ok := rMap.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 1, aggVals.Size())

	// Call Finalize() to reset the Results.
	res.Finalize()

	// Ensure data was removed by call to Finalize().
	aggVals, ok = rMap.Get(ident.StringID("foo"))
	require.False(t, ok)
	require.Nil(t, aggVals.Map())
	require.Equal(t, 0, res.Size())

	for _, entry := range rMap.Iter() {
		id := entry.Key()
		require.False(t, id.IsNoFinalize())
	}
}
