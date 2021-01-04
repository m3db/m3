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

package index_test

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"

	"github.com/stretchr/testify/require"
)

func TestBatchMatcherAllowPartialReflexive(t *testing.T) {
	a := index.Batch{
		AllowPartialUpdates: true,
	}
	b := index.Batch{
		AllowPartialUpdates: false,
	}
	require.False(t, index.NewBatchMatcher(a).Matches(b))
	require.False(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherLengthReflexive(t *testing.T) {
	a := index.Batch{
		Docs: []doc.Metadata{
			{},
		},
	}
	b := index.Batch{}
	require.False(t, index.NewBatchMatcher(a).Matches(b))
	require.False(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherSameDoc(t *testing.T) {
	testDoc := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	a := index.Batch{
		Docs: []doc.Metadata{testDoc, testDoc},
	}
	b := index.Batch{
		Docs: []doc.Metadata{testDoc, testDoc},
	}
	require.True(t, index.NewBatchMatcher(a).Matches(b))
	require.True(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherOrderMatters(t *testing.T) {
	testDocA := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	testDocB := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("bar"),
				Value: []byte("foo"),
			},
		},
	}
	a := index.Batch{
		Docs: []doc.Metadata{testDocA, testDocB},
	}
	b := index.Batch{
		Docs: []doc.Metadata{testDocB, testDocA},
	}
	require.False(t, index.NewBatchMatcher(a).Matches(b))
	require.False(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherSameDocs(t *testing.T) {
	testDocA := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	testDocB := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("bar"),
				Value: []byte("foo"),
			},
		},
	}
	a := index.Batch{
		Docs: []doc.Metadata{testDocA, testDocB},
	}
	b := index.Batch{
		Docs: []doc.Metadata{testDocA, testDocB},
	}
	require.True(t, index.NewBatchMatcher(a).Matches(b))
	require.True(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherDocFieldsDiffer(t *testing.T) {
	testDocA := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	testDocB := doc.Metadata{
		ID: []byte("abc"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar1"),
			},
		},
	}
	a := index.Batch{
		Docs: []doc.Metadata{testDocA},
	}
	b := index.Batch{
		Docs: []doc.Metadata{testDocB},
	}
	require.False(t, index.NewBatchMatcher(a).Matches(b))
	require.False(t, index.NewBatchMatcher(b).Matches(a))
}

func TestBatchMatcherDocIDsDiffer(t *testing.T) {
	testDocA := doc.Metadata{
		ID: []byte("abc1"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	testDocB := doc.Metadata{
		ID: []byte("abc2"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
		},
	}
	a := index.Batch{
		Docs: []doc.Metadata{testDocA},
	}
	b := index.Batch{
		Docs: []doc.Metadata{testDocB},
	}
	require.False(t, index.NewBatchMatcher(a).Matches(b))
	require.False(t, index.NewBatchMatcher(b).Matches(a))
}
