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
	"testing"

	"github.com/m3db/m3ninx/doc"
	sgmt "github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/index/util"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/postings/roaring"

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
)

func TestSimpleE2EConstruction(t *testing.T) {
	newTestSegments(t, nil)
}

func TestConstructionWithFewDocs(t *testing.T) {
	newTestSegments(t, fewTestDocuments)
}

func TestConstructionWithLotsOfDocs(t *testing.T) {
	newTestSegments(t, lotsTestDocuments)
}

func TestSizeEqualsWithFewDocs(t *testing.T) {
	memSeg, fstSeg := newTestSegments(t, fewTestDocuments)
	require.Equal(t, memSeg.Size(), fstSeg.Size())
}

func TestSizeEqualsWithLotsOfDocs(t *testing.T) {
	memSeg, fstSeg := newTestSegments(t, lotsTestDocuments)
	require.Equal(t, memSeg.Size(), fstSeg.Size())
}

func TestFieldsEqualsWithFewDocs(t *testing.T) {
	testFieldsEquals(t, fewTestDocuments)
}

func TestFieldsEqualsWithLotsDocs(t *testing.T) {
	testFieldsEquals(t, lotsTestDocuments)
}

func TestTermEqualsWithFewDocs(t *testing.T) {
	testTermsEqual(t, fewTestDocuments)
}

func TestTermEqualsWithLotsDocs(t *testing.T) {
	testTermsEqual(t, lotsTestDocuments)
}

func TestPostingsListEqualForMatchTermFewDocs(t *testing.T) {
	testPostingsListEqualForMatchTerm(t, fewTestDocuments)
}

func TestPostingsListEqualForMatchTermLotsDocs(t *testing.T) {
	testPostingsListEqualForMatchTerm(t, lotsTestDocuments)
}

func TestPostingsListContainsIDFewDocs(t *testing.T) {
	testPostingsListContainsID(t, fewTestDocuments)
}

func TestPostingsListContainsIDLotsDocs(t *testing.T) {
	testPostingsListContainsID(t, lotsTestDocuments)
}

func TestPostingsListRegexAllFewDocs(t *testing.T) {
	testPostingsListRegexAll(t, fewTestDocuments)
}

func TestPostingsListRegexAllLotsDocs(t *testing.T) {
	testPostingsListRegexAll(t, lotsTestDocuments)
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

func testPostingsListRegexAll(t *testing.T, docs []doc.Document) {
	memSeg, fstSeg := newTestSegments(t, docs)
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
}

func testPostingsListContainsID(t *testing.T, docs []doc.Document) {
	memSeg, fstSeg := newTestSegments(t, docs)
	memIDs, err := memSeg.Terms(doc.IDReservedFieldName)
	require.NoError(t, err)
	for _, i := range memIDs {
		ok, err := fstSeg.ContainsID(i)
		require.NoError(t, err)
		require.True(t, ok)
	}
}

func testPostingsListEqualForMatchTerm(t *testing.T, docs []doc.Document) {
	memSeg, fstSeg := newTestSegments(t, docs)
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

func testTermsEqual(t *testing.T, docs []doc.Document) {
	memSeg, fstSeg := newTestSegments(t, docs)

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
}

func testFieldsEquals(t *testing.T, docs []doc.Document) {
	memSeg, fstSeg := newTestSegments(t, docs)

	memFields, err := memSeg.Fields()
	require.NoError(t, err)

	fstFields, err := fstSeg.Fields()
	require.NoError(t, err)

	assertSliceOfByteSlicesEqual(t, memFields, fstFields)
}

func assertSliceOfByteSlicesEqual(t *testing.T, a, b [][]byte) {
	require.Equal(t, len(a), len(b), fmt.Sprintf("a = [%s], b = [%s]", pprint(a), pprint(b)))
	sortSliceOfByteSlices(a)
	sortSliceOfByteSlices(b)
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
		postingsBuffer  bytes.Buffer
		fstTermsBuffer  bytes.Buffer
		fstFieldsBuffer bytes.Buffer
	)

	require.NoError(t, w.WritePostingsOffsets(&postingsBuffer))
	require.NoError(t, w.WriteFSTTerms(&fstTermsBuffer))
	require.NoError(t, w.WriteFSTFields(&fstFieldsBuffer))

	data := SegmentData{
		MajorVersion:  w.MajorVersion(),
		MinorVersion:  w.MinorVersion(),
		Metadata:      w.Metadata(),
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
