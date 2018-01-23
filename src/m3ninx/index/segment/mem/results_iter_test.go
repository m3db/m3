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
	"fmt"
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

func acceptAllDocs(d doc.Document) bool { return true }
func rejectAllDocs(d doc.Document) bool { return false }

func TestResultsIterEmpty(t *testing.T) {
	iter := newResultsIter(nil, acceptAllDocs, nil)
	require.False(t, iter.Next())
}

func TestResultsSingleResult(t *testing.T) {
	pl := segment.NewPostingsList()
	pl.Insert(1)
	sq := &stubQueryable{
		opts: NewOptions(),
		docs: []document{
			document{
				docID: 1,
				Document: doc.Document{
					ID: doc.ID("abcdef"),
				},
			},
		},
	}

	iter := newResultsIter(pl, acceptAllDocs, sq)
	require.True(t, iter.Next())
	next, tombstoned := iter.Current()
	require.False(t, tombstoned)
	require.Equal(t, doc.ID("abcdef"), next.ID)
	require.False(t, iter.Next())
}

func TestResultsSingleResultFiltering(t *testing.T) {
	pl := segment.NewPostingsList()
	pl.Insert(1)
	sq := &stubQueryable{
		opts: NewOptions(),
		docs: []document{
			document{
				docID: 1,
				Document: doc.Document{
					ID: doc.ID("abcdef"),
				},
			},
		},
	}

	iter := newResultsIter(pl, rejectAllDocs, sq)
	require.False(t, iter.Next())
}

func TestResultsIterWithFiltering(t *testing.T) {
	pl := segment.NewPostingsList()
	pl.Insert(1)
	pl.Insert(2)
	sq := &stubQueryable{
		opts: NewOptions(),
		docs: []document{
			document{
				docID: 1,
				Document: doc.Document{
					ID: doc.ID("abcdef"),
				},
			},
			document{
				docID: 2,
				Document: doc.Document{
					ID: doc.ID("foobar"),
				},
			},
		},
	}

	filterFn := func(d doc.Document) bool { return string(d.ID) == "foobar" }
	iter := newResultsIter(pl, filterFn, sq)
	require.True(t, iter.Next())
	next, tombstoned := iter.Current()
	require.False(t, tombstoned)
	require.Equal(t, doc.ID("foobar"), next.ID)
	require.False(t, iter.Next())
}

type stubQueryable struct {
	opts Options
	docs []document
}

func (s *stubQueryable) Options() Options { return s.opts }

func (s *stubQueryable) Filter(f segment.Filter) (candidateDocIDs segment.PostingsList, pendingFilterFn matchPredicate, err error) {
	return nil, nil, nil
}

func (s *stubQueryable) FetchDocument(docID segment.DocID) (document, error) {
	for _, d := range s.docs {
		if docID == d.docID {
			return d, nil
		}
	}
	return document{}, fmt.Errorf("unknown DocID")
}
