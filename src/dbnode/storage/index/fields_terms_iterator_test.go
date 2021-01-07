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
	"sort"
	"strings"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/util"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testFstOptions    = fst.NewOptions()
	lotsTestDocuments = util.MustReadDocs("../../../m3ninx/util/testdata/node_exporter.json", 2000)
)

func TestFieldsTermsIteratorSimple(t *testing.T) {
	s := newFieldsTermsIterSetup(
		pair{"a", "b"}, pair{"a", "c"},
		pair{"d", "e"}, pair{"d", "f"},
		pair{"g", "h"},
		pair{"i", "j"},
		pair{"k", "l"},
	)
	reader, err := s.asSegment(t).Reader()
	require.NoError(t, err)

	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{iterateTerms: true})
	require.NoError(t, err)
	s.requireEquals(t, iter)
}

func TestFieldsTermsIteratorReuse(t *testing.T) {
	pairs := []pair{
		{"a", "b"},
		{"a", "c"},
		{"d", "e"},
		{"d", "f"},
		{"g", "h"},
		{"i", "j"},
		{"k", "l"},
	}

	iter, err := newFieldsAndTermsIterator(nil, fieldsAndTermsIteratorOpts{})
	require.NoError(t, err)

	s := newFieldsTermsIterSetup(pairs...)
	reader, err := s.asSegment(t).Reader()
	require.NoError(t, err)

	err = iter.Reset(reader, fieldsAndTermsIteratorOpts{iterateTerms: true})
	require.NoError(t, err)
	s.requireEquals(t, iter)

	err = iter.Reset(reader, fieldsAndTermsIteratorOpts{
		iterateTerms: true,
		allowFn: func(f []byte) bool {
			return !bytes.Equal([]byte("a"), f) && !bytes.Equal([]byte("k"), f)
		},
	})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{
		{"d", "e"},
		{"d", "f"},
		{"g", "h"},
		{"i", "j"},
	}, slice)

	err = iter.Reset(reader, fieldsAndTermsIteratorOpts{
		iterateTerms: true,
		allowFn: func(f []byte) bool {
			return bytes.Equal([]byte("k"), f) || bytes.Equal([]byte("a"), f)
		},
	})
	require.NoError(t, err)
	slice = toSlice(t, iter)
	requireSlicesEqual(t, []pair{
		{"a", "b"},
		{"a", "c"},
		{"k", "l"},
	}, slice)
}

func TestFieldsTermsIteratorSimpleSkip(t *testing.T) {
	input := []pair{
		{"a", "b"},
		{"a", "c"},
		{"d", "e"},
		{"d", "f"},
		{"g", "h"},
		{"i", "j"},
		{"k", "l"},
	}
	s := newFieldsTermsIterSetup(input...)
	reader, err := s.asSegment(t).Reader()
	require.NoError(t, err)

	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{
		iterateTerms: true,
		allowFn: func(f []byte) bool {
			return !bytes.Equal([]byte("a"), f) && !bytes.Equal([]byte("k"), f)
		},
	})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{
		{"d", "e"},
		{"d", "f"},
		{"g", "h"},
		{"i", "j"},
	}, slice)
}

func TestFieldsTermsIteratorTermsOnly(t *testing.T) {
	s := newFieldsTermsIterSetup(
		pair{"a", "b"},
		pair{"a", "c"},
		pair{"d", "e"},
		pair{"d", "f"},
		pair{"g", "h"},
		pair{"i", "j"},
		pair{"k", "l"},
	)
	reader, err := s.asSegment(t).Reader()
	require.NoError(t, err)

	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{
		{"a", ""},
		{"d", ""},
		{"g", ""},
		{"i", ""},
		{"k", ""},
	}, slice)
}

func TestFieldsTermsIteratorEmptyTerm(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	reader := newMockSegmentReader(ctrl, map[string][]string{
		"a": nil,
	})
	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{iterateTerms: false})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{{"a", ""}}, slice)
}

func TestFieldsTermsIteratorEmptyTermInclude(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	reader := newMockSegmentReader(ctrl, map[string][]string{
		"a": nil,
	})
	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{iterateTerms: true})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{}, slice)
}

func TestFieldsTermsIteratorIterateTermsAndRestrictByQuery(t *testing.T) {
	testDocs := []doc.Metadata{
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

	seg, err := mem.NewSegment(mem.NewOptions())
	require.NoError(t, err)

	require.NoError(t, seg.InsertBatch(m3ninxindex.Batch{
		Docs:                testDocs,
		AllowPartialUpdates: true,
	}))

	require.NoError(t, seg.Seal())

	fruitRegexp, err := idx.NewRegexpQuery([]byte("fruit"), []byte("^.*apple$"))
	require.NoError(t, err)

	colorRegexp, err := idx.NewRegexpQuery([]byte("color"), []byte("^(red|yellow)$"))
	require.NoError(t, err)

	reader, err := seg.Reader()
	require.NoError(t, err)

	iter, err := newFieldsAndTermsIterator(reader, fieldsAndTermsIteratorOpts{
		iterateTerms: true,
		restrictByQuery: &Query{
			Query: idx.NewConjunctionQuery(fruitRegexp, colorRegexp),
		},
	})
	require.NoError(t, err)
	slice := toSlice(t, iter)
	requireSlicesEqual(t, []pair{
		{"color", "red"},
		{"color", "yellow"},
		{"fruit", "apple"},
		{"fruit", "pineapple"},
	}, slice)
}

func newMockSegmentReader(ctrl *gomock.Controller, tagValues map[string][]string) segment.Reader {
	fields := make([]iterpoint, 0, len(tagValues))
	for k := range tagValues {
		fields = append(fields, iterpoint{
			value: k,
		})
	}
	sort.Slice(fields, func(i, j int) bool {
		return strings.Compare(fields[i].value, fields[j].value) < 0
	})

	r := segment.NewMockReader(ctrl)
	fieldIterator := &stubFieldIterator{points: fields}

	r.EXPECT().Fields().Return(fieldIterator, nil).AnyTimes()

	for _, f := range fields {
		termValues := tagValues[f.value]
		sort.Strings(termValues)
		terms := make([]iterpoint, 0, len(termValues))
		for _, t := range termValues {
			terms = append(terms, iterpoint{
				value: t,
			})
		}
		termIterator := &stubTermIterator{points: terms}
		r.EXPECT().Terms([]byte(f.value)).Return(termIterator, nil).AnyTimes()
	}

	return r
}

type stubTermIterator struct {
	current iterpoint
	points  []iterpoint
}

func (s *stubTermIterator) Next() bool {
	if len(s.points) == 0 {
		return false
	}
	s.current = s.points[0]
	s.points = s.points[1:]
	return true
}

func (s *stubTermIterator) Current() ([]byte, postings.List) {
	return []byte(s.current.value), nil
}

func (s *stubTermIterator) Err() error {
	return s.current.err
}

func (s *stubTermIterator) Close() error {
	if s.current.err != nil {
		return s.current.err
	}
	for s.Next() {
		if err := s.Err(); err != nil {
			return err
		}
	}
	return nil
}

type stubFieldIterator struct {
	current iterpoint
	points  []iterpoint
}

func (s *stubFieldIterator) Next() bool {
	if len(s.points) == 0 {
		return false
	}
	s.current = s.points[0]
	s.points = s.points[1:]
	return true
}

func (s *stubFieldIterator) Current() []byte {
	return []byte(s.current.value)
}

func (s *stubFieldIterator) Err() error {
	return s.current.err
}

func (s *stubFieldIterator) Close() error {
	if s.current.err != nil {
		return s.current.err
	}
	for s.Next() {
		if err := s.Err(); err != nil {
			return err
		}
	}
	return nil
}

type iterpoint struct {
	err   error
	value string
}

type pair struct {
	Name, Value string
}

func newFieldsTermsIterSetup(fields ...pair) fieldsTermsIterSetup {
	sort.Slice(fields, func(i, j int) bool {
		c := strings.Compare(fields[i].Name, fields[j].Name)
		if c == 0 {
			return strings.Compare(fields[i].Value, fields[j].Value) < 0
		}
		return c < 0
	})
	return fieldsTermsIterSetup{fields}
}

type fieldsTermsIterSetup struct {
	fields []pair
}

func (s *fieldsTermsIterSetup) asSegment(t *testing.T) segment.Segment {
	docs := make([]doc.Metadata, 0, len(s.fields))
	for _, f := range s.fields {
		docs = append(docs, doc.Metadata{
			ID: []byte(fmt.Sprintf("id_%v_%v", f.Name, f.Value)),
			Fields: []doc.Field{
				{
					Name:  []byte(f.Name),
					Value: []byte(f.Value),
				},
			},
		})
	}
	memSeg := testSegment(t, docs...).(segment.MutableSegment)
	return fst.ToTestSegment(t, memSeg, testFstOptions)
}

func (s *fieldsTermsIterSetup) requireEquals(t *testing.T, iter fieldsAndTermsIterator) {
	pending := s.fields
	for len(pending) > 0 {
		require.True(t, iter.Next())
		name, value := iter.Current()
		if bytes.Equal(name, doc.IDReservedFieldName) {
			continue
		}
		top := pending[0]
		pending = pending[1:]
		require.Equal(t, top.Name, string(name))
		require.Equal(t, top.Value, string(value))
	}
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func toSlice(t *testing.T, iter fieldsAndTermsIterator) []pair {
	var pairs []pair
	for iter.Next() {
		n, v := iter.Current()
		if bytes.Equal(n, doc.IDReservedFieldName) {
			continue
		}
		pairs = append(pairs, pair{
			Name:  string(n),
			Value: string(v),
		})
	}
	return pairs
}

func requireSlicesEqual(t *testing.T, a, b []pair) {
	require.Equal(t, len(a), len(b))
	for i := 0; i < len(a); i++ {
		require.Equal(t, a[i], b[i])
	}
}
