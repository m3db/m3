// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	"bytes"
	re "regexp"
	"testing"

	"github.com/m3db/m3ninx/doc"

	"github.com/stretchr/testify/require"
)

func TestSegmentInsert(t *testing.T) {
	tests := []struct {
		name  string
		input doc.Document
	}{
		{
			name: "document without an ID",
			input: doc.Document{
				Fields: []doc.Field{
					doc.Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
		},
		{
			name: "document with an ID",
			input: doc.Document{
				Fields: []doc.Field{
					doc.Field{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
					doc.Field{
						Name:  doc.IDReservedFieldName,
						Value: []byte("123"),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			segment, err := NewSegment(0, NewOptions())
			require.NoError(t, err)

			err = segment.Insert(test.input)
			require.NoError(t, err)

			reader, err := segment.Reader()
			require.NoError(t, err)

			name, value := test.input.Fields[0].Name, test.input.Fields[0].Value
			pl, err := reader.MatchTerm(name, value)
			require.NoError(t, err)

			iter, err := reader.Docs(pl)
			require.NoError(t, err)

			require.True(t, iter.Next())
			actual := iter.Current()
			require.True(t, compareDocs(test.input, actual))

			// The document must have an ID.
			_, ok := actual.Get(doc.IDReservedFieldName)
			require.True(t, ok)

			require.False(t, iter.Next())
			require.NoError(t, iter.Err())
			require.NoError(t, iter.Close())
		})
	}
}

func TestSegmentInsertTwice(t *testing.T) {
	var (
		id    = []byte("123")
		first = doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
				doc.Field{
					Name:  doc.IDReservedFieldName,
					Value: id,
				},
			},
		}
		second = doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
				doc.Field{
					Name:  []byte("variety"),
					Value: []byte("fuji"),
				},
				doc.Field{
					Name:  doc.IDReservedFieldName,
					Value: id,
				},
			},
		}
	)

	segment, err := NewSegment(0, NewOptions())
	require.NoError(t, err)

	err = segment.Insert(first)
	require.NoError(t, err)

	reader, err := segment.Reader()
	require.NoError(t, err)

	pl, err := reader.MatchTerm(doc.IDReservedFieldName, id)
	require.NoError(t, err)

	iter, err := reader.Docs(pl)
	require.NoError(t, err)

	require.True(t, iter.Next())
	actual := iter.Current()

	// Only the first document should be indexed.
	require.True(t, compareDocs(first, actual))
	require.False(t, compareDocs(second, actual))

	require.NoError(t, iter.Close())
}

func TestSegmentReaderMatchExact(t *testing.T) {
	docs := []doc.Document{
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
				doc.Field{
					Name:  doc.IDReservedFieldName,
					Value: []byte("83"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("green"),
				},
			},
		},
	}

	segment, err := NewSegment(0, NewOptions())
	require.NoError(t, err)

	for _, doc := range docs {
		err = segment.Insert(doc)
		require.NoError(t, err)
	}

	reader, err := segment.Reader()
	require.NoError(t, err)

	pl, err := reader.MatchTerm([]byte("fruit"), []byte("apple"))
	require.NoError(t, err)

	iter, err := reader.Docs(pl)
	require.NoError(t, err)

	actualDocs := make([]doc.Document, 0)
	for iter.Next() {
		actualDocs = append(actualDocs, iter.Current())
	}

	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	expectedDocs := []doc.Document{docs[0], docs[2]}
	require.Equal(t, len(expectedDocs), len(actualDocs))
	for i := range actualDocs {
		require.True(t, compareDocs(expectedDocs[i], actualDocs[i]))
	}
}

func TestSegmentReaderMatchRegex(t *testing.T) {
	docs := []doc.Document{
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("pineapple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
				doc.Field{
					Name:  doc.IDReservedFieldName,
					Value: []byte("42"),
				},
			},
		},
	}

	segment, err := NewSegment(0, NewOptions())
	require.NoError(t, err)

	for _, doc := range docs {
		err = segment.Insert(doc)
		require.NoError(t, err)
	}

	reader, err := segment.Reader()
	require.NoError(t, err)

	field, regexp := []byte("fruit"), []byte(".*ple")
	compiled := re.MustCompile(string(regexp))
	pl, err := reader.MatchRegexp(field, regexp, compiled)
	require.NoError(t, err)

	iter, err := reader.Docs(pl)
	require.NoError(t, err)

	actualDocs := make([]doc.Document, 0)
	for iter.Next() {
		actualDocs = append(actualDocs, iter.Current())
	}

	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	expectedDocs := []doc.Document{docs[1], docs[2]}
	require.Equal(t, len(expectedDocs), len(actualDocs))
	for i := range actualDocs {
		require.True(t, compareDocs(expectedDocs[i], actualDocs[i]))
	}
}

// compareDocs returns whether two documents are equal. If only one of the documents
// contains an ID the ID is excluded from the comparison since it was auto-generated.
func compareDocs(l, r doc.Document) bool {
	lIdx, lOK := hasID(l)
	rIdx, rOK := hasID(r)
	if !exclusiveOr(lOK, rOK) {
		return l.Equal(r)
	}
	if lOK {
		l = removeID(l, lIdx)
	}
	if rOK {
		r = removeID(r, rIdx)
	}
	return l.Equal(r)
}

func hasID(d doc.Document) (int, bool) {
	for i, f := range d.Fields {
		if bytes.Equal(f.Name, doc.IDReservedFieldName) {
			return i, true
		}
	}
	return 0, false
}

func removeID(d doc.Document, idx int) doc.Document {
	cp := make([]doc.Field, 0, len(d.Fields))
	for i, f := range d.Fields {
		if i == idx {
			continue
		}
		cp = append(cp, f)
	}
	return doc.Document{
		Fields: cp,
	}
}

func exclusiveOr(a, b bool) bool {
	return (a || b) && !(a && b)
}
