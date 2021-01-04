// Copyright (c) 2021  Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/postings"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDocIterator(t *testing.T) {
	mockCtrl := xtest.NewController(t)
	defer mockCtrl.Finish()

	docs := []doc.Metadata{
		{
			ID: []byte("doc-id-1"),
			Fields: []doc.Field{
				{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
		},
		{
			ID: []byte("doc-id-2"),
			Fields: []doc.Field{
				{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
		},
	}

	encodedDocsWithIds := make([]docsWithIDs, 0, len(docs))
	for i, d := range docs {
		encodedDocsWithIds = append(encodedDocsWithIds, docsWithIDs{
			id: postings.ID(i),
			doc: doc.NewDocumentFromEncoded(
				doc.Encoded{Bytes: docToBytes(d)}),
		})
	}

	retriever := NewMockDocRetriever(mockCtrl)
	gomock.InOrder(
		retriever.EXPECT().Doc(encodedDocsWithIds[0].id).Return(encodedDocsWithIds[0].doc, nil),
		retriever.EXPECT().Doc(encodedDocsWithIds[1].id).Return(encodedDocsWithIds[1].doc, nil),
	)

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(encodedDocsWithIds[0].id),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(encodedDocsWithIds[1].id),
		postingsIter.EXPECT().Next().Return(false),
		postingsIter.EXPECT().Close().Return(nil),
	)

	it := NewIterator(retriever, postingsIter)

	require.True(t, it.Next())
	require.Equal(t, encodedDocsWithIds[0].doc, it.Current())
	require.True(t, it.Next())
	require.Equal(t, encodedDocsWithIds[1].doc, it.Current())
	require.False(t, it.Next())
	require.NoError(t, it.Err())

	require.NoError(t, it.Close())
}

type docsWithIDs struct {
	id  postings.ID
	doc doc.Document
}

func docToBytes(d doc.Metadata) []byte {
	enc := encoding.NewEncoder(1024)
	n := enc.PutBytes(d.ID)
	n += enc.PutUvarint(uint64(len(d.Fields)))
	for _, f := range d.Fields { // nolint:gocritic
		n += enc.PutBytes(f.Name)
		n += enc.PutBytes(f.Value)
	}

	return enc.Bytes()
}
