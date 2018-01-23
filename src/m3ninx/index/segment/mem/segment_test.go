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
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type newSegmentFn func() Segment

type segmentTestSuite struct {
	suite.Suite

	fn      newSegmentFn
	segment Segment
}

func (t *segmentTestSuite) SetupTest() {
	t.segment = t.fn()
}

func (t *segmentTestSuite) TestInsert() {
	err := t.segment.Insert(doc.Document{
		ID: []byte("abc=efg"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("efg")},
		},
	})
	t.NoError(err)

	docs, err := t.segment.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efg"),
				Negate:           false,
				Regexp:           false,
			},
		},
	})
	t.NoError(err)
	t.True(docs != nil)
	t.True(docs.Next())
	result, tombstoned := docs.Current()
	t.Equal(doc.ID("abc=efg"), result.ID)
	t.False(tombstoned)
	t.False(docs.Next())
}

func (t *segmentTestSuite) TestQueryRegex() {
	err := t.segment.Insert(doc.Document{
		ID: []byte("abc=efg"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("efg")},
		},
	})
	t.NoError(err)

	err = t.segment.Insert(doc.Document{
		ID: []byte("abc=efgh"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)

	docs, err := t.segment.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efg.+"),
				Negate:           false,
				Regexp:           true,
			},
		},
	})
	t.NoError(err)
	t.True(docs != nil)
	t.True(docs.Next())
	result, tombstoned := docs.Current()
	t.Equal(doc.ID("abc=efgh"), result.ID)
	t.False(tombstoned)
	t.False(docs.Next())
}

func (t *segmentTestSuite) TestQueryNegate() {
	err := t.segment.Insert(doc.Document{
		ID: []byte("abc=efg"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("efg")},
		},
	})
	t.NoError(err)

	err = t.segment.Insert(doc.Document{
		ID: []byte("abc=efgh"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("abc"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)

	docs, err := t.segment.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efg"),
				Negate:           true,
				Regexp:           false,
			},
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efgh"),
				Negate:           false,
				Regexp:           false,
			},
		},
	})
	t.NoError(err)
	t.True(docs != nil)
	t.True(docs.Next())
	result, tombstoned := docs.Current()
	t.Equal(doc.ID("abc=efgh"), result.ID)
	t.False(tombstoned)
	t.False(docs.Next())
}

func (t *segmentTestSuite) TestQueryRegexNegate() {
	err := t.segment.Insert(doc.Document{
		ID: []byte("abc=efg|foo=bar"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("abc"), Value: doc.Value("efg")},
		},
	})
	t.NoError(err)

	err = t.segment.Insert(doc.Document{
		ID: []byte("abc=efgh|foo=baz"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("abc"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)
	err = t.segment.Insert(doc.Document{
		ID: []byte("foo=bar|baz=efgh"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("baz"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)

	docs, err := t.segment.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("foo"),
				FieldValueFilter: []byte("bar"),
				Negate:           false,
				Regexp:           false,
			},
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efg.+"),
				Negate:           true,
				Regexp:           true,
			},
		},
	})
	t.NoError(err)
	t.True(docs != nil)

	returnedIds := []doc.ID{}
	for docs.Next() {
		result, tombstoned := docs.Current()
		t.False(tombstoned)
		returnedIds = append(returnedIds, result.ID)
	}
	t.Equal(2, len(returnedIds))
	requiredIds := []doc.ID{
		doc.ID("abc=efg|foo=bar"),
		doc.ID("foo=bar|baz=efgh"),
	}
	for _, req := range requiredIds {
		found := false
		for _, ret := range returnedIds {
			if string(req) == string(ret) {
				found = true
			}
		}
		t.True(found, string(req))
	}
}

func (t *segmentTestSuite) TestQueryExactNegate() {
	err := t.segment.Insert(doc.Document{
		ID: []byte("abc=efg|foo=bar"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("abc"), Value: doc.Value("efg")},
		},
	})
	t.NoError(err)

	err = t.segment.Insert(doc.Document{
		ID: []byte("abc=efgh|foo=baz"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("abc"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)
	err = t.segment.Insert(doc.Document{
		ID: []byte("foo=bar|baz=efgh"),
		Fields: []doc.Field{
			doc.Field{Name: []byte("foo"), Value: doc.Value("bar")},
			doc.Field{Name: []byte("baz"), Value: doc.Value("efgh")},
		},
	})
	t.NoError(err)

	docs, err := t.segment.Query(segment.Query{
		Conjunction: segment.AndConjunction,
		Filters: []segment.Filter{
			segment.Filter{
				FieldName:        []byte("foo"),
				FieldValueFilter: []byte("bar"),
				Negate:           false,
				Regexp:           false,
			},
			segment.Filter{
				FieldName:        []byte("abc"),
				FieldValueFilter: []byte("efgh"),
				Negate:           true,
				Regexp:           false,
			},
		},
	})
	t.NoError(err)

	returnedIds := []doc.ID{}
	for docs.Next() {
		result, tombstoned := docs.Current()
		t.False(tombstoned)
		returnedIds = append(returnedIds, result.ID)
	}
	t.Equal(2, len(returnedIds))

	requiredIds := []doc.ID{
		doc.ID("abc=efg|foo=bar"),
		doc.ID("foo=bar|baz=efgh"),
	}
	for _, req := range requiredIds {
		found := false
		for _, ret := range returnedIds {
			if string(req) == string(ret) {
				found = true
			}
		}
		t.True(found, string(req))
	}
}

// TODO(prateek): test for write idempotency
// TODO(prateek): test for update
// TODO(prateek): test for delete
// TODO(prateek): test for insert after delete
// TODO(prateek): SUT property tests for Segment
// TODO(prateek): property tests to ensure all Segments give equal results for any input (within reason - mutability)
// TODO(prateek): these are all "integration" style unit tests, add unit tests for individuals components w/ mocks

func TestSimpleSegment(t *testing.T) {
	fn := func() Segment {
		opts := NewOptions()
		seg, err := New(1, opts)
		require.NoError(t, err)
		return seg
	}
	suite.Run(t, &segmentTestSuite{fn: fn})
}
