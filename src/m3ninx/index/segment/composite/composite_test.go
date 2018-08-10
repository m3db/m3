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

package composite

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/index/util"
	"github.com/m3db/m3/src/m3ninx/postings"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testOptions = NewOptions()

	fewTestDocuments = []doc.Document{
		doc.Document{
			ID: []byte("yellowisaterriblecolor"),
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
			ID: []byte("fruitsandothers"),
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

func TestCompositeSegmentDocRanges(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	s1 := sgmt.NewMockSegment(ctrl)
	r1 := index.NewMockReader(ctrl)
	r1.EXPECT().DocRange().Return(postings.ID(10), postings.ID(21)) // i.e. 10 docs
	s1.EXPECT().Reader().Return(r1, nil)

	s2 := sgmt.NewMockSegment(ctrl)
	r2 := index.NewMockReader(ctrl)
	r2.EXPECT().DocRange().Return(postings.ID(11), postings.ID(15)) // i.e 4 docs
	s2.EXPECT().Reader().Return(r2, nil)

	compSeg, err := NewSegment(testOptions, s1, s2)
	require.NoError(t, err)

	reader, err := compSeg.Reader()
	require.NoError(t, err)
	min, max := reader.DocRange()
	require.Equal(t, 0, int(min))
	require.Equal(t, 15, int(max))
}

func TestCompositeSegmentClose(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	s1 := sgmt.NewMockSegment(ctrl)
	r1 := index.NewMockReader(ctrl)
	r1.EXPECT().DocRange().Return(postings.ID(0), postings.ID(1))
	s1.EXPECT().Reader().Return(r1, nil)

	s2 := sgmt.NewMockSegment(ctrl)
	r2 := index.NewMockReader(ctrl)
	r2.EXPECT().DocRange().Return(postings.ID(0), postings.ID(1))
	s2.EXPECT().Reader().Return(r2, nil)

	compSeg, err := NewSegment(testOptions, s1, s2)
	require.NoError(t, err)

	s1.EXPECT().Close().Return(nil)
	r1.EXPECT().Close().Return(nil)
	s2.EXPECT().Close().Return(nil)
	r2.EXPECT().Close().Return(nil)
	require.NoError(t, compSeg.Close())
}

func TestCompositeSegmentSize(t *testing.T) {
	for _, td := range testDocuments {
		t.Run(td.name, func(t *testing.T) {
			memSeg := newTestMemSegment(t, td.docs)
			for i := 1; i < 6; i++ {
				testSeg := newTestCompositeSegment(t, i, td.docs)
				require.Equal(t, memSeg.Size(), testSeg.Size())
			}
		})
	}
}

func TestCompositeSegmentFieldsEquals(t *testing.T) {
	for _, td := range testDocuments {
		for numSplits := 1; numSplits < 6; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				memSeg := newTestMemSegment(t, td.docs)
				memFieldsIter, err := memSeg.Fields()
				require.NoError(t, err)
				memFields := toSlice(t, memFieldsIter)

				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				compFieldsIter, err := compSeg.Fields()
				require.NoError(t, err)
				compFields := toSlice(t, compFieldsIter)
				assertSliceOfByteSlicesEqual(t, memFields, compFields)
			})
		}
	}
}

func TestCompositeSegmentTermsEquals(t *testing.T) {
	for _, td := range testDocuments {
		memSeg := newTestMemSegment(t, td.docs)
		memFieldsIter, err := memSeg.Fields()
		require.NoError(t, err)
		memFields := toSlice(t, memFieldsIter)

		for numSplits := 1; numSplits < 6; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				for _, f := range memFields {
					memTermsIter, err := memSeg.Terms(f)
					require.NoError(t, err)
					memTerms := toSlice(t, memTermsIter)
					compTermsIter, err := compSeg.Terms(f)
					require.NoError(t, err)
					compTerms := toSlice(t, compTermsIter)
					assertSliceOfByteSlicesEqual(t, memTerms, compTerms)
				}
			})
		}
	}
}

func TestCompositeSegmentContainsID(t *testing.T) {
	for _, td := range testDocuments {
		for numSplits := 1; numSplits < 6; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				for _, d := range td.docs {
					ok, err := compSeg.ContainsID(d.ID)
					require.NoError(t, err)
					require.True(t, ok)
				}
			})
		}
	}
}

func TestCompositeSegmentMatchesAll(t *testing.T) {
	for _, td := range testDocuments {
		for numSplits := 1; numSplits < 6; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				reader, err := compSeg.Reader()
				require.NoError(t, err)
				docIter, err := reader.AllDocs()
				require.NoError(t, err)
				dm := doc.NewIteratorMatcher(false, td.docs...)
				require.True(t, dm.Matches(docIter))
			})
		}
	}
}

func TestCompositeSegmentMatchTerm(t *testing.T) {
	for _, td := range testDocuments {
		memSeg := newTestMemSegment(t, td.docs)
		memReader, err := memSeg.Reader()
		require.NoError(t, err)
		memFieldsIter, err := memSeg.Fields()
		require.NoError(t, err)
		memFields := toSlice(t, memFieldsIter)
		for numSplits := 1; numSplits < 6; numSplits++ {
			t.Run(fmt.Sprintf("%s/numSplits=%d", td.name, numSplits), func(t *testing.T) {
				compSeg := newTestCompositeSegment(t, numSplits, td.docs)
				compReader, err := compSeg.Reader()
				require.NoError(t, err)

				for _, field := range memFields {
					memTermsIter, err := memSeg.Terms(field)
					require.NoError(t, err)
					memTerms := toSlice(t, memTermsIter)
					for _, term := range memTerms {
						pl, err := memReader.MatchTerm(field, term)
						require.NoError(t, err)
						memDocsIter, err := memReader.Docs(pl)
						require.NoError(t, err)
						memDocs := toDocSlice(t, memDocsIter)
						matcher := doc.NewIteratorMatcher(false, memDocs...)

						cPl, err := compReader.MatchTerm(field, term)
						require.NoError(t, err)
						compDocsIter, err := compReader.Docs(cPl)
						require.NoError(t, err)
						require.True(t, matcher.Matches(compDocsIter))
					}
				}
			})
		}
	}
}

func toDocSlice(t *testing.T, it doc.Iterator) []doc.Document {
	var docs []doc.Document
	for it.Next() {
		d := it.Current()
		var copied doc.Document
		copied.ID = append([]byte(nil), d.ID...)
		for _, f := range d.Fields {
			copied.Fields = append(copied.Fields, doc.Field{
				Name:  append([]byte(nil), f.Name...),
				Value: append([]byte(nil), f.Value...),
			})
		}
		docs = append(docs, copied)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return docs
}

func assertSliceOfByteSlicesEqual(t *testing.T, a, b [][]byte) {
	require.Equal(t, len(a), len(b), fmt.Sprintf("a = [%s], b = [%s]", pprint(a), pprint(b)))
	require.Equal(t, a, b)
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

func newTestCompositeSegment(t *testing.T, numSegments int, docs []doc.Document) sgmt.Segment {
	splits := make([][]doc.Document, numSegments)
	for i := 0; i < len(docs); i++ {
		idx := i % numSegments
		splits[idx] = append(splits[idx], docs[i])
	}
	segs := make([]sgmt.Segment, 0, numSegments)
	for i := 0; i < numSegments; i++ {
		seg := newTestMemSegment(t, splits[i])
		segs = append(segs, seg)
	}
	c, err := NewSegment(testOptions, segs...)
	require.NoError(t, err)
	return c
}

func newTestMemSegment(t *testing.T, docs []doc.Document) sgmt.Segment {
	opts := mem.NewOptions()
	s, err := mem.NewSegment(postings.ID(0), opts)
	require.NoError(t, err)
	for _, d := range docs {
		_, err := s.Insert(d)
		require.NoError(t, err)
	}
	_, err = s.Seal()
	require.NoError(t, err)
	return s
}
