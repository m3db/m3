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

package fst

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/util"

	"github.com/stretchr/testify/require"
)

var (
	testOptions = NewOptions()

	fewTestDocuments = []doc.Metadata{
		{
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("banana"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
		{
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("apple"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("red"),
				},
			},
		},
		{
			ID: []byte("42"),
			Fields: []doc.Field{
				{
					Name:  []byte("fruit"),
					Value: []byte("pineapple"),
				},
				{
					Name:  []byte("color"),
					Value: []byte("yellow"),
				},
			},
		},
	}
	lotsTestDocuments = util.MustReadDocs("../../../util/testdata/node_exporter.json", 2000)

	testDocuments = []struct {
		name string
		docs []doc.Metadata
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

type testSegmentCase struct {
	name               string
	expected, observed sgmt.Segment
}

func newTestCases(t *testing.T, docs []doc.Metadata) []testSegmentCase {
	memSeg, fstSeg := newTestSegments(t, docs)

	fstWriter10Reader10 := newFSTSegmentWithVersion(t, memSeg, testOptions,
		Version{Major: 1, Minor: 0}, /* writer version */
		Version{Major: 1, Minor: 0} /* reader version */)

	fstWriter11Reader10 := newFSTSegmentWithVersion(t, memSeg, testOptions,
		Version{Major: 1, Minor: 1}, /* writer version */
		Version{Major: 1, Minor: 0} /* reader version */)

	fstWriter11Reader11 := newFSTSegmentWithVersion(t, memSeg, testOptions,
		Version{Major: 1, Minor: 1}, /* writer version */
		Version{Major: 1, Minor: 1} /* reader version */)

	return []testSegmentCase{
		{ // mem sgmt v latest fst
			name:     "mem v fst",
			expected: memSeg,
			observed: fstSeg,
		},
		{ // mem sgmt v fst1.0
			name:     "mem v fstWriter10Reader10",
			expected: memSeg,
			observed: fstWriter10Reader10,
		},
		{ // mem sgmt v fst (WriterV1.1; ReaderV1.0) -- i.e. ensure forward compatibility
			name:     "mem v fstWriter11Reader10",
			expected: memSeg,
			observed: fstWriter11Reader10,
		},
		{ // mem sgmt v fst (WriterV1.1; ReaderV1.1)
			name:     "mem v fstWriter11Reader11",
			expected: memSeg,
			observed: fstWriter11Reader11,
		},
	}
}

func TestConstruction(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					// don't need to do anything here
				})
			}
		})
	}
}

func TestSizeEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					require.Equal(t, expSeg.Size(), obsSeg.Size())
				})
			}
		})
	}
}

func TestFieldDoesNotExist(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					elaborateFieldName := []byte("some-elaborate-field-that-does-not-exist-in-test-docs")
					terms, err := tc.expected.TermsIterable().Terms(elaborateFieldName)
					require.NoError(t, err)
					require.False(t, terms.Next())
					require.NoError(t, terms.Err())
					require.NoError(t, terms.Close())

					terms, err = tc.observed.TermsIterable().Terms(elaborateFieldName)
					require.NoError(t, err)
					require.False(t, terms.Next())
					require.NoError(t, terms.Err())
					require.NoError(t, terms.Close())

					expectedReader, err := tc.expected.Reader()
					require.NoError(t, err)
					pl, err := expectedReader.MatchTerm(elaborateFieldName, []byte("."))
					require.NoError(t, err)
					require.True(t, pl.IsEmpty())
					pl, err = expectedReader.MatchTerm(elaborateFieldName, []byte(".*"))
					require.NoError(t, err)
					require.True(t, pl.IsEmpty())
					require.NoError(t, expectedReader.Close())

					observedReader, err := tc.observed.Reader()
					require.NoError(t, err)
					pl, err = observedReader.MatchTerm(elaborateFieldName, []byte("."))
					require.NoError(t, err)
					require.True(t, pl.IsEmpty())
					pl, err = observedReader.MatchTerm(elaborateFieldName, []byte(".*"))
					require.NoError(t, err)
					require.True(t, pl.IsEmpty())
					require.NoError(t, observedReader.Close())
				})
			}
		})
	}
}

func TestFieldsEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expFieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					expFields := toSlice(t, expFieldsIter)

					obsFieldsIter, err := obsSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					obsFields := toSlice(t, obsFieldsIter)

					assertSliceOfByteSlicesEqual(t, expFields, obsFields)
				})
			}
		})
	}
}

func TestContainsField(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expFieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					expFields := toSlice(t, expFieldsIter)

					for _, f := range expFields {
						ok, err := obsSeg.ContainsField(f)
						require.NoError(t, err)
						require.True(t, ok)
					}
				})
			}
		})
	}
}

func TestTermEquals(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expFieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					expFields := toSlice(t, expFieldsIter)

					obsFieldsIter, err := obsSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					obsFields := toSlice(t, obsFieldsIter)

					assertTermEquals := func(fields [][]byte) {
						for _, f := range fields {
							expTermsIter, err := expSeg.TermsIterable().Terms(f)
							require.NoError(t, err)
							expTerms := toTermPostings(t, expTermsIter)

							obsTermsIter, err := obsSeg.TermsIterable().Terms(f)
							require.NoError(t, err)
							obsTerms := toTermPostings(t, obsTermsIter)
							require.Equal(t, expTerms, obsTerms)
						}
					}
					assertTermEquals(expFields)
					assertTermEquals(obsFields)
				})
			}
		})
	}
}

func TestPostingsListEqualForMatchField(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expReader, err := expSeg.Reader()
					require.NoError(t, err)
					obsReader, err := obsSeg.Reader()
					require.NoError(t, err)

					expFieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					expFields := toSlice(t, expFieldsIter)

					for _, f := range expFields {
						expPl, err := expReader.MatchField(f)
						require.NoError(t, err)
						obsPl, err := obsReader.MatchField(f)
						require.NoError(t, err)
						require.True(t, expPl.Equal(obsPl),
							fmt.Sprintf("field[%s] - [%v] != [%v]", string(f), pprintIter(expPl), pprintIter(obsPl)))
					}
				})
			}
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

			memFieldsIter, err := memSeg.Fields()
			require.NoError(t, err)
			memFields := toSlice(t, memFieldsIter)

			for _, f := range memFields {
				memTermsIter, err := memSeg.Terms(f)
				require.NoError(t, err)
				memTerms := toTermPostings(t, memTermsIter)

				for term := range memTerms {
					memPl, err := memReader.MatchTerm(f, []byte(term))
					require.NoError(t, err)
					fstPl, err := fstReader.MatchTerm(f, []byte(term))
					require.NoError(t, err)
					require.True(t, memPl.Equal(fstPl),
						fmt.Sprintf("%s:%s - [%v] != [%v]", string(f), term, pprintIter(memPl), pprintIter(fstPl)))
				}
			}
		})
	}
}

func TestPostingsListContainsID(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expIDsIter, err := expSeg.TermsIterable().Terms(doc.IDReservedFieldName)
					require.NoError(t, err)
					expIDs := toTermPostings(t, expIDsIter)
					for i := range expIDs {
						ok, err := obsSeg.ContainsID([]byte(i))
						require.NoError(t, err)
						require.True(t, ok)
					}
				})
			}
		})
	}
}

func TestPostingsListRegexAll(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					fieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					fields := toSlice(t, fieldsIter)
					for _, f := range fields {
						reader, err := expSeg.Reader()
						require.NoError(t, err)
						c, err := index.CompileRegex([]byte(".*"))
						require.NoError(t, err)
						expPl, err := reader.MatchRegexp(f, c)
						require.NoError(t, err)

						obsReader, err := obsSeg.Reader()
						require.NoError(t, err)
						c, err = index.CompileRegex([]byte(".*"))
						require.NoError(t, err)
						obsPl, err := obsReader.MatchRegexp(f, c)
						require.NoError(t, err)
						require.True(t, expPl.Equal(obsPl))
					}
				})
			}
		})
	}
}

func TestSegmentDocs(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expReader, err := expSeg.Reader()
					require.NoError(t, err)
					obsReader, err := obsSeg.Reader()
					require.NoError(t, err)

					expFieldsIter, err := expSeg.FieldsIterable().Fields()
					require.NoError(t, err)
					expFields := toSlice(t, expFieldsIter)

					for _, f := range expFields {
						expTermsIter, err := expSeg.TermsIterable().Terms(f)
						require.NoError(t, err)
						expTerms := toTermPostings(t, expTermsIter)

						for term := range expTerms {
							expPl, err := expReader.MatchTerm(f, []byte(term))
							require.NoError(t, err)
							obsPl, err := obsReader.MatchTerm(f, []byte(term))
							require.NoError(t, err)

							expDocs, err := expReader.MetadataIterator(expPl)
							require.NoError(t, err)
							obsDocs, err := obsReader.MetadataIterator(obsPl)
							require.NoError(t, err)

							assertDocsEqual(t, expDocs, obsDocs)
						}
					}
				})
			}
		})
	}
}

func TestSegmentAllDocs(t *testing.T) {
	for _, test := range testDocuments {
		t.Run(test.name, func(t *testing.T) {
			for _, tc := range newTestCases(t, test.docs) {
				t.Run(tc.name, func(t *testing.T) {
					expSeg, obsSeg := tc.expected, tc.observed
					expReader, err := expSeg.Reader()
					require.NoError(t, err)
					obsReader, err := obsSeg.Reader()
					require.NoError(t, err)
					expDocs, err := expReader.AllDocs()
					require.NoError(t, err)
					obsDocs, err := obsReader.AllDocs()
					require.NoError(t, err)
					assertDocsEqual(t, expDocs, obsDocs)
				})
			}
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
				fstSeg.FieldsIterable().Fields()
				wg.Done()
			}()
			go func() {
				fstSeg.FieldsIterable().Fields()
				wg.Done()
			}()
			wg.Wait()
		})
	}
}

func TestPostingsListLifecycleSimple(t *testing.T) {
	_, fstSeg := newTestSegments(t, fewTestDocuments)
	require.NoError(t, fstSeg.Close())

	_, err := fstSeg.FieldsIterable().Fields()
	require.Error(t, err)

	_, err = fstSeg.TermsIterable().Terms(nil)
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

func TestSegmentReaderValidUntilClose(t *testing.T) {
	_, fstSeg := newTestSegments(t, fewTestDocuments)

	reader, err := fstSeg.Reader()
	require.NoError(t, err)

	// Close segment early, expect reader still valid until close.
	err = fstSeg.Close()
	require.NoError(t, err)

	// Make sure all methods allow for calls until the reader is closed.
	var (
		list postings.List
	)
	list, err = reader.MatchField([]byte("fruit"))
	require.NoError(t, err)
	assertPostingsList(t, list, []postings.ID{0, 1, 2})

	list, err = reader.MatchTerm([]byte("color"), []byte("yellow"))
	require.NoError(t, err)
	assertPostingsList(t, list, []postings.ID{0, 2})

	re, err := index.CompileRegex([]byte("^.*apple$"))
	require.NoError(t, err)
	list, err = reader.MatchRegexp([]byte("fruit"), re)
	require.NoError(t, err)
	assertPostingsList(t, list, []postings.ID{1, 2})

	list, err = reader.MatchAll()
	require.NoError(t, err)
	assertPostingsList(t, list, []postings.ID{0, 1, 2})

	_, err = reader.Metadata(0)
	require.NoError(t, err)

	_, err = reader.MetadataIterator(list)
	require.NoError(t, err)

	_, err = reader.AllDocs()
	require.NoError(t, err)

	// Test returned iterators also work
	re, err = index.CompileRegex([]byte("^.*apple$"))
	require.NoError(t, err)
	list, err = reader.MatchRegexp([]byte("fruit"), re)
	require.NoError(t, err)
	iter, err := reader.MetadataIterator(list)
	require.NoError(t, err)
	var docs int
	for iter.Next() {
		docs++
		var fruitField doc.Field
		for _, field := range iter.Current().Fields {
			if bytes.Equal(field.Name, []byte("fruit")) {
				fruitField = field
				break
			}
		}
		require.True(t, bytes.HasSuffix(fruitField.Value, []byte("apple")))
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	// Now close.
	require.NoError(t, reader.Close())

	// Make sure reader now starts returning errors.
	_, err = reader.MatchTerm([]byte("color"), []byte("yellow"))
	require.Error(t, err)
}

func newTestSegments(t *testing.T, docs []doc.Metadata) (memSeg sgmt.MutableSegment, fstSeg sgmt.Segment) {
	s := newTestMemSegment(t)
	for _, d := range docs {
		_, err := s.Insert(d)
		require.NoError(t, err)
	}
	return s, newFSTSegment(t, s, testOptions)
}

func newTestMemSegment(t *testing.T) sgmt.MutableSegment {
	opts := mem.NewOptions()
	s, err := mem.NewSegment(opts)
	require.NoError(t, err)
	return s
}

func assertSliceOfByteSlicesEqual(t *testing.T, a, b [][]byte) {
	require.Equal(t, len(a), len(b), fmt.Sprintf("a = [%s], b = [%s]", pprint(a), pprint(b)))
	require.Equal(t, a, b)
}

func assertDocsEqual(t *testing.T, a, b doc.MetadataIterator) {
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

func assertPostingsList(t *testing.T, l postings.List, exp []postings.ID) {
	it := l.Iterator()

	defer func() {
		require.False(t, it.Next(), "should exhaust just once")
		require.NoError(t, it.Err(), "should not complete with error")
		require.NoError(t, it.Close(), "should not encounter error on close")
	}()

	match := make(map[postings.ID]struct{}, len(exp))
	for _, v := range exp {
		match[v] = struct{}{}
	}

	for it.Next() {
		curr := it.Current()

		_, ok := match[curr]
		if !ok {
			require.Fail(t,
				fmt.Sprintf("expected %d, not found in postings iter", curr))
			return
		}

		delete(match, curr)
	}

	if len(match) == 0 {
		// Success.
		return
	}

	remaining := make([]int, 0, len(match))
	for id := range match {
		remaining = append(remaining, int(id))
	}

	msg := fmt.Sprintf("unmatched expected IDs %v, not found in postings iter",
		remaining)
	require.Fail(t, msg)
}

func collectDocs(iter doc.MetadataIterator) ([]doc.Metadata, error) {
	var docs []doc.Metadata
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

func toSlice(t *testing.T, iter sgmt.OrderedBytesIterator) [][]byte {
	elems := [][]byte{}
	for iter.Next() {
		curr := iter.Current()
		bytes := append([]byte(nil), curr...)
		elems = append(elems, bytes)
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return elems
}

type termPostings map[string][]int

func toTermPostings(t *testing.T, iter sgmt.TermsIterator) termPostings {
	elems := make(termPostings)
	for iter.Next() {
		term, postings := iter.Current()
		_, exists := elems[string(term)]
		require.False(t, exists)

		values := []int{}
		it := postings.Iterator()
		for it.Next() {
			values = append(values, int(it.Current()))
		}
		sort.Sort(sort.IntSlice(values))

		require.NoError(t, it.Err())
		require.NoError(t, it.Close())

		elems[string(term)] = values
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return elems
}
