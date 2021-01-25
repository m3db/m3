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

package proptest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/m3ninx/doc"
	idxdocs "github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
)

type documentIteratorMatcher struct {
	expectedDocs map[string]doc.Document
	t            *testing.T
}

func newDocumentIteratorMatcher(t *testing.T, docs ...doc.Document) (*documentIteratorMatcher, error) {
	docMap := make(map[string]doc.Document, len(docs))
	for _, d := range docs {
		rawID, err := idxdocs.ReadIDFromDocument(d)
		id := string(rawID)
		require.NoError(t, err)
		if _, ok := docMap[id]; ok {
			return nil, fmt.Errorf("received document with duplicate id: %v", d)
		}
		docMap[id] = d
	}
	return &documentIteratorMatcher{
		expectedDocs: docMap,
		t:            t,
	}, nil
}

// Matches returns whether the provided iterator matches the collection of provided docs.
func (m *documentIteratorMatcher) Matches(i doc.Iterator) error {
	pendingDocIDs := make(map[string]doc.Document, len(m.expectedDocs))
	for id := range m.expectedDocs {
		pendingDocIDs[id] = m.expectedDocs[id]
	}
	for i.Next() {
		d := i.Current()
		rawID, err := idxdocs.ReadIDFromDocument(d)
		require.NoError(m.t, err)
		id := string(rawID)
		expectedDoc, ok := m.expectedDocs[id]
		if !ok {
			return fmt.Errorf("received un-expected document: %+v", d)
		}
		if !m.compareDocs(expectedDoc, d) {
			return fmt.Errorf("received document: %+v did not match expected doc %+v", d, expectedDoc)
		}
		delete(pendingDocIDs, id)
	}
	if err := i.Err(); err != nil {
		return fmt.Errorf("unexpected iterator error: %v", err)
	}
	if err := i.Close(); err != nil {
		return fmt.Errorf("unexpected iterator close error: %v", err)
	}
	if len(pendingDocIDs) > 0 {
		return fmt.Errorf("did not receive docs: %+v", pendingDocIDs)
	}
	return nil
}

func (m *documentIteratorMatcher) compareDocs(d1 doc.Document, d2 doc.Document) bool {
	docReader := idxdocs.NewEncodedDocumentReader()
	d1Metadata, err := idxdocs.MetadataFromDocument(d1, docReader)
	require.NoError(m.t, err)

	d2Metadata, err := idxdocs.MetadataFromDocument(d2, docReader)
	require.NoError(m.t, err)

	return d1Metadata.Equal(d2Metadata)
}
