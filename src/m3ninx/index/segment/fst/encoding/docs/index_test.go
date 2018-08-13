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

package docs

import (
	"bytes"
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/stretchr/testify/require"
)

func TestStoredFieldsIndex(t *testing.T) {
	type entry struct {
		id     postings.ID
		offset uint64
	}

	tests := []struct {
		name    string
		entries []entry
	}{
		{
			name:    "no offsets",
			entries: []entry{},
		},
		{
			name: "single offset",
			entries: []entry{
				entry{id: 0, offset: 0},
			},
		},
		{
			name: "single offset with non-zero base",
			entries: []entry{
				entry{id: 65, offset: 12},
			},
		},
		{
			name: "multiple offsets",
			entries: []entry{
				entry{id: 0, offset: 0},
				entry{id: 1, offset: 20},
				entry{id: 6, offset: 50},
				entry{id: 18, offset: 100},
				entry{id: 19, offset: 125},
			},
		},
		{
			name: "multiple offsets with non-zero base",
			entries: []entry{
				entry{id: 42, offset: 0},
				entry{id: 43, offset: 33},
				entry{id: 44, offset: 77},
				entry{id: 45, offset: 88},
				entry{id: 49, offset: 111},
			},
		},
		{
			name: "non-monotonic offsets",
			entries: []entry{
				entry{id: 13, offset: 124},
				entry{id: 14, offset: 28},
				entry{id: 15, offset: 95},
				entry{id: 17, offset: 56},
				entry{id: 20, offset: 77},
			},
		},
	}

	w := NewIndexWriter(nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			w.Reset(buf)

			for i := range test.entries {
				id, offset := test.entries[i].id, test.entries[i].offset
				err := w.Write(id, offset)
				require.NoError(t, err)
			}

			r, err := NewIndexReader(buf.Bytes())
			require.NoError(t, err)
			for i := range test.entries {
				if i == 0 {
					require.Equal(t, test.entries[i].id, r.Base())
				}
				id, offset := test.entries[i].id, test.entries[i].offset
				actual, err := r.Read(id)
				require.NoError(t, err)
				require.Equal(t, offset, actual)
			}

			var expectedLen int
			if len(test.entries) == 0 {
				expectedLen = 0
			} else {
				start := test.entries[0].id
				end := test.entries[len(test.entries)-1].id
				expectedLen = int(end-start) + 1
			}
			require.Equal(t, expectedLen, r.Len())
		})
	}
}
