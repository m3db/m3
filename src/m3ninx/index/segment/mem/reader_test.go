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

package mem

import (
	re "regexp"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestReaderMatchExact(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	maxID := postings.ID(55)

	name, value := []byte("apple"), []byte("red")
	postingsList := roaring.NewPostingsList()
	require.NoError(t, postingsList.Insert(postings.ID(42)))
	require.NoError(t, postingsList.Insert(postings.ID(50)))
	require.NoError(t, postingsList.Insert(postings.ID(57)))

	segment := NewMockReadableSegment(mockCtrl)
	gomock.InOrder(
		segment.EXPECT().matchTerm(name, value).Return(postingsList, nil),
	)

	reader := newReader(segment, readerDocRange{0, maxID}, postings.NewPool(nil, roaring.NewPostingsList))

	actual, err := reader.MatchTerm(name, value)
	require.NoError(t, err)
	require.True(t, postingsList.Equal(actual))

	require.NoError(t, reader.Close())
}

func TestReaderMatchRegex(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	maxID := postings.ID(55)

	name, regexp := []byte("apple"), []byte("r.*")
	compiled := re.MustCompile(string(regexp))
	postingsList := roaring.NewPostingsList()
	require.NoError(t, postingsList.Insert(postings.ID(42)))
	require.NoError(t, postingsList.Insert(postings.ID(50)))
	require.NoError(t, postingsList.Insert(postings.ID(57)))

	segment := NewMockReadableSegment(mockCtrl)
	gomock.InOrder(
		segment.EXPECT().matchRegexp(name, compiled).Return(postingsList, nil),
	)

	reader := newReader(segment, readerDocRange{0, maxID}, postings.NewPool(nil, roaring.NewPostingsList))
	actual, err := reader.MatchRegexp(name, index.CompiledRegex{Simple: compiled})
	require.NoError(t, err)
	require.True(t, postingsList.Equal(actual))

	require.NoError(t, reader.Close())
}

func TestReaderMatchAll(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		minID = postings.ID(42)
		maxID = postings.ID(47)
	)

	postingsList := roaring.NewPostingsList()
	require.NoError(t, postingsList.Insert(postings.ID(42)))
	require.NoError(t, postingsList.Insert(postings.ID(43)))
	require.NoError(t, postingsList.Insert(postings.ID(44)))
	require.NoError(t, postingsList.Insert(postings.ID(45)))
	require.NoError(t, postingsList.Insert(postings.ID(46)))

	reader := newReader(nil, readerDocRange{minID, maxID}, postings.NewPool(nil, roaring.NewPostingsList))

	actual, err := reader.MatchAll()
	require.NoError(t, err)
	require.True(t, postingsList.Equal(actual))

	require.NoError(t, reader.Close())
}

func TestReaderDocs(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	maxID := postings.ID(50)
	docs := []doc.Metadata{
		{
			Fields: []doc.Field{
				{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
		},
		{
			Fields: []doc.Field{
				{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
		},
	}

	segment := NewMockReadableSegment(mockCtrl)
	gomock.InOrder(
		segment.EXPECT().getDoc(postings.ID(42)).Return(docs[0], nil),
		segment.EXPECT().getDoc(postings.ID(47)).Return(docs[1], nil),
	)

	postingsList := roaring.NewPostingsList()
	require.NoError(t, postingsList.Insert(postings.ID(42)))
	require.NoError(t, postingsList.Insert(postings.ID(47)))
	require.NoError(t, postingsList.Insert(postings.ID(57))) // IDs past maxID should be ignored.

	reader := newReader(segment, readerDocRange{0, maxID}, postings.NewPool(nil, roaring.NewPostingsList))

	iter, err := reader.MetadataIterator(postingsList)
	require.NoError(t, err)

	actualDocs := make([]doc.Metadata, 0, len(docs))
	for iter.Next() {
		actualDocs = append(actualDocs, iter.Current())
	}

	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	require.Equal(t, docs, actualDocs)

	require.NoError(t, reader.Close())
}

func TestReaderAllDocs(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	maxID := postings.ID(2)
	docs := []doc.Metadata{
		{
			Fields: []doc.Field{
				{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
		},
		{
			Fields: []doc.Field{
				{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
		},
	}

	segment := NewMockReadableSegment(mockCtrl)
	gomock.InOrder(
		segment.EXPECT().getDoc(postings.ID(0)).Return(docs[0], nil),
		segment.EXPECT().getDoc(postings.ID(1)).Return(docs[1], nil),
	)

	reader := newReader(segment, readerDocRange{0, maxID}, postings.NewPool(nil, roaring.NewPostingsList))
	iter, err := reader.AllDocs()
	require.NoError(t, err)

	actualDocs := make([]doc.Metadata, 0, len(docs))
	for iter.Next() {
		actualDocs = append(actualDocs, iter.Current())
	}

	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	require.Equal(t, docs, actualDocs)

	require.NoError(t, reader.Close())
}
