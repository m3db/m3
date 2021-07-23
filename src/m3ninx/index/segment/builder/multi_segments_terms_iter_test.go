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

package builder

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/stretchr/testify/require"
)

func TestTermsIterFromSegmentsDeduplicates(t *testing.T) {
	segments := []segment.Segment{
		newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("foo"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
		}),
		newTestSegmentWithDocs(t, []doc.Metadata{
			{
				ID: []byte("bar"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("color"), Value: []byte("blue")},
					{Name: []byte("alpha"), Value: []byte("1.0")},
				},
			},
			{
				ID: []byte("foo"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
				},
			},
			{
				ID: []byte("baz"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("watermelon")},
					{Name: []byte("color"), Value: []byte("green")},
					{Name: []byte("alpha"), Value: []byte("0.5")},
				},
			},
			{
				ID: []byte("bux"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("watermelon")},
					{Name: []byte("color"), Value: []byte("red")},
					{Name: []byte("alpha"), Value: []byte("0.1")},
				},
			},
		}),
	}

	builder := NewBuilderFromSegments(testOptions)
	builder.Reset()
	require.NoError(t, builder.AddSegments(segments))
	iter, err := builder.Terms([]byte("fruit"))
	require.NoError(t, err)

	assertTermsPostings(t, builder.Docs(), iter, termPostings{
		"apple":      []int{0, 1},
		"watermelon": []int{2, 3},
	})
}

func assertTermsPostings(
	t *testing.T,
	docs []doc.Metadata,
	iter segment.TermsIterator,
	expected termPostings,
) {
	actual := toTermPostings(t, iter)
	require.Equal(t, expected, actual,
		fmt.Sprintf("actual terms postings:\n%s",
			termsPostingsString(docs, actual)))
}

func termsPostingsString(docs []doc.Metadata, tp termPostings) string {
	str := strings.Builder{}
	for k, ids := range tp {
		str.WriteString(k)
		str.WriteString(":\n")
		for _, id := range ids {
			str.WriteString("id=")
			str.WriteString(strconv.Itoa(id))
			str.WriteString(", doc=")

			if id >= len(docs) {
				msg := fmt.Sprintf("cannot find doc: postingsID=%d, numDocs=%d", id, len(docs))
				panic(msg)
			}

			doc := docs[postings.ID(id)]
			str.WriteString(doc.String())
			str.WriteString("\n")
		}
		str.WriteString("\n")
	}
	return str.String()
}
