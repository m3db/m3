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

// Package test is a package for shared test helpers.
package test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
)

// DocumentToTagIter is a help for converting a doc.Document into an
// ident.TagIterator.
func DocumentToTagIter(t *testing.T, doc doc.Document) ident.TagIterator {
	reader := docs.NewEncodedDocumentReader()
	m, err := docs.MetadataFromDocument(doc, reader)
	require.NoError(t, err)

	return convert.ToSeriesTags(m, convert.Opts{NoClone: true})
}
