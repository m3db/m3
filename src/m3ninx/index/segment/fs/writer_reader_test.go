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

package fs

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/index/util"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"

	"github.com/stretchr/testify/require"
)

var (
	fewTestDocuments = []doc.Document{
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
			ID: []byte("42"),
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("fruit"),
					Value: []byte("pineapple"),
				},
				doc.Field{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
	}
	lotsTestDocuments = util.MustReadDocs("../../util/testdata/node_exporter.json", 2000)

	testDocuments = []struct {
		name string
		docs []doc.Document
	}{
		{
			name: "few documents",
			docs: fewTestDocuments,
		},
		{
			name: "many documents",
			docs: lotsTestDocuments,
		},
	}
)

func TestConstruction(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			newTestSegments(t, test.docs)
		})
	}
}

func TestSizeEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			require.Equal(t, memSeg.Size(), fstSeg.Size())
		})
	}
}

func TestFieldDoesNotExist(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)

			elaborateFieldName := []byte("some-elaborate-field-that-does-not-exist-in-test-docs")
			terms, err := memSeg.Terms(elaborateFieldName)
			require.NoError(t, err)
			require.Nil(t, terms)

			terms, err = fstSeg.Terms(elaborateFieldName)
			require.NoError(t, err)
			require.Nil(t, terms)

			memReader, err := memSeg.Reader()
			require.NoError(t, err)
			pl, err := memReader.MatchTerm(elaborateFieldName, []byte("."))
			require.NoError(t, err)
			require.True(t, pl.IsEmpty())
			pl, err = memReader.MatchTerm(elaborateFieldName, []byte(".*"))
			require.NoError(t, err)
			require.True(t, pl.IsEmpty())
			require.NoError(t, memReader.Close())

			fstReader, err := fstSeg.Reader()
			require.NoError(t, err)
			pl, err = fstReader.MatchTerm(elaborateFieldName, []byte("."))
			require.NoError(t, err)
			require.True(t, pl.IsEmpty())
			pl, err = fstReader.MatchTerm(elaborateFieldName, []byte(".*"))
			require.NoError(t, err)
			require.True(t, pl.IsEmpty())
			require.NoError(t, fstReader.Close())

		})
	}
}

func TestFieldsEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)

			memFields, err := memSeg.Fields()
			require.NoError(t, err)

			fstFields, err := fstSeg.Fields()
			require.NoError(t, err)

			assertSliceOfByteSlicesEqual(t, memFields, fstFields)

		})
	}
}

func TestFieldsEqualsParallel(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			_, fstSeg := newTestSegments(t, test.docs)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				fstSeg.Fields()
				wg.Done()
			}()
			go func() {
				fstSeg.Fields()
				wg.Done()
			}()
			wg.Wait()
		})
	}
}

func TestTermEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)

			memFields, err := memSeg.Fields()
			require.NoError(t, err)
			fstFields, err := fstSeg.Fields()
			require.NoError(t, err)

			assertTermEquals := func(fields [][]byte) {
				for _, f := range fields {
					memTerms, err := memSeg.Terms(f)
					require.NoError(t, err)
					fstTerms, err := fstSeg.Terms(f)
					require.NoError(t, err)
					assertSliceOfByteSlicesEqual(t, memTerms, fstTerms)
				}
			}
			assertTermEquals(memFields)
			assertTermEquals(fstFields)

		})
	}
}

func TestPostingsListEqualForMatchTerm(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			memReader, err := memSeg.Reader()
			require.NoError(t, err)
			fstReader, err := fstSeg.Reader()
			require.NoError(t, err)

			memFields, err := memSeg.Fields()
			require.NoError(t, err)

			for _, f := range memFields {
				memTerms, err := memSeg.Terms(f)
				require.NoError(t, err)

				for _, term := range memTerms {
					memPl, err := memReader.MatchTerm(f, term)
					require.NoError(t, err)
					fstPl, err := fstReader.MatchTerm(f, term)
					require.NoError(t, err)
					require.True(t, memPl.Equal(fstPl),
						fmt.Sprintf("%s:%s - [%v] != [%v]", string(f), string(term), pprintIter(memPl), pprintIter(fstPl)))
				}
			}
		})
	}
}

func TestPostingsListContainsID(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			memIDs, err := memSeg.Terms(doc.IDReservedFieldName)
			require.NoError(t, err)
			for _, i := range memIDs {
				ok, err := fstSeg.ContainsID(i)
				require.NoError(t, err)
				require.True(t, ok)
			}
		})
	}
}

func TestPostingsListRegexAll(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			fields, err := memSeg.Fields()
			require.NoError(t, err)
			for _, f := range fields {
				reader, err := memSeg.Reader()
				require.NoError(t, err)
				memPl, err := reader.MatchRegexp(f, []byte("."), nil)
				require.NoError(t, err)

				fstReader, err := fstSeg.Reader()
				require.NoError(t, err)
				fstPl, err := fstReader.MatchRegexp(f, []byte(".*"), nil)
				require.NoError(t, err)
				require.True(t, memPl.Equal(fstPl))
			}
		})
	}
}

func TestSegmentDocs(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			memReader, err := memSeg.Reader()
			require.NoError(t, err)
			fstReader, err := fstSeg.Reader()
			require.NoError(t, err)

			memFields, err := memSeg.Fields()
			require.NoError(t, err)

			for _, f := range memFields {
				memTerms, err := memSeg.Terms(f)
				require.NoError(t, err)

				for _, term := range memTerms {
					memPl, err := memReader.MatchTerm(f, term)
					require.NoError(t, err)
					fstPl, err := fstReader.MatchTerm(f, term)
					require.NoError(t, err)

					memDocs, err := memReader.Docs(memPl)
					require.NoError(t, err)
					fstDocs, err := fstReader.Docs(fstPl)
					require.NoError(t, err)

					assertDocsEqual(t, memDocs, fstDocs)
				}
			}
		})
	}
}

func TestSegmentAllDocs(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			memSeg, fstSeg := newTestSegments(t, test.docs)
			memReader, err := memSeg.Reader()
			require.NoError(t, err)
			fstReader, err := fstSeg.Reader()
			require.NoError(t, err)

			memDocs, err := memReader.AllDocs()
			require.NoError(t, err)
			fstDocs, err := fstReader.AllDocs()
			require.NoError(t, err)

			assertDocsEqual(t, memDocs, fstDocs)
		})
	}
}

func TestPostingsListLifecycleSimple(t *testing.T) {
	_, fstSeg := newTestSegments(t, fewTestDocuments)

	require.NoError(t, fstSeg.Close())

	_, err := fstSeg.Fields()
	require.Error(t, err)

	_, err = fstSeg.Terms(nil)
	require.Error(t, err)

	_, err = fstSeg.Reader()
	require.Error(t, err)
}

func TestPostingsListReaderLifecycle(t *testing.T) {
	_, fstSeg := newTestSegments(t, fewTestDocuments)
	reader, err := fstSeg.Reader()
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	_, err = fstSeg.Reader()
	require.NoError(t, err)
}

func newTestSegments(t *testing.T, docs []doc.Document) (memSeg sgmt.MutableSegment, fstSeg sgmt.Segment) {
	s := newTestMemSegment(t)
	for _, d := range docs {
		_, err := s.Insert(d)
		require.NoError(t, err)
	}
	return s, newFSTSegment(t, s)
}

func newTestMemSegment(t *testing.T) sgmt.MutableSegment {
	opts := mem.NewOptions()
	s, err := mem.NewSegment(postings.ID(0), opts)
	require.NoError(t, err)
	return s
}

func newFSTSegment(t *testing.T, s sgmt.MutableSegment) sgmt.Segment {
	_, err := s.Seal()
	require.NoError(t, err)

	w := NewWriter()
	require.NoError(t, w.Reset(s))

	var (
		docsDataBuffer  bytes.Buffer
		docsIndexBuffer bytes.Buffer
		postingsBuffer  bytes.Buffer
		fstTermsBuffer  bytes.Buffer
		fstFieldsBuffer bytes.Buffer
	)

	require.NoError(t, w.WriteDocumentsData(&docsDataBuffer))
	require.NoError(t, w.WriteDocumentsIndex(&docsIndexBuffer))
	require.NoError(t, w.WritePostingsOffsets(&postingsBuffer))
	require.NoError(t, w.WriteFSTTerms(&fstTermsBuffer))
	require.NoError(t, w.WriteFSTFields(&fstFieldsBuffer))

	data := SegmentData{
		MajorVersion:  w.MajorVersion(),
		MinorVersion:  w.MinorVersion(),
		Metadata:      w.Metadata(),
		DocsData:      docsDataBuffer.Bytes(),
		DocsIdxData:   docsIndexBuffer.Bytes(),
		PostingsData:  postingsBuffer.Bytes(),
		FSTTermsData:  fstTermsBuffer.Bytes(),
		FSTFieldsData: fstFieldsBuffer.Bytes(),
	}
	opts := NewSegmentOpts{
		PostingsListPool: postings.NewPool(nil, roaring.NewPostingsList),
	}
	reader, err := NewSegment(data, opts)
	require.NoError(t, err)

	return reader
}

func assertSliceOfByteSlicesEqual(t *testing.T, a, b [][]byte) {
	require.Equal(t, len(a), len(b), fmt.Sprintf("a = [%s], b = [%s]", pprint(a), pprint(b)))
	sortSliceOfByteSlices(a)
	sortSliceOfByteSlices(b)
	require.Equal(t, a, b)
}

func assertDocsEqual(t *testing.T, a, b doc.Iterator) {
	aDocs, err := collectDocs(a)
	require.NoError(t, err)
	bDocs, err := collectDocs(b)
	require.NoError(t, err)

	require.Equal(t, len(aDocs), len(bDocs))

	sort.Sort(doc.Documents(aDocs))
	sort.Sort(doc.Documents(bDocs))

	for i := range aDocs {
		require.True(t, aDocs[i].Equal(bDocs[i]))
	}
}

func collectDocs(iter doc.Iterator) ([]doc.Document, error) {
	var docs []doc.Document
	for iter.Next() {
		docs = append(docs, iter.Current())
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return docs, nil
}

func pprint(a [][]byte) string {
	var buf bytes.Buffer
	for i, t := range a {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%d %s", i, string(t)))
	}
	return buf.String()
}

func pprintIter(pl postings.List) string {
	var buf bytes.Buffer
	iter := pl.Iterator()
	for i := 0; iter.Next(); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%d", iter.Current()))
	}
	return buf.String()
}
