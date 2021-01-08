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

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/util"

	"github.com/stretchr/testify/require"
)

var tests = []struct {
	name string
	docs []doc.Metadata
}{
	{
		name: "empty document",
		docs: []doc.Metadata{
			{
				Fields: doc.Fields{},
			},
		},
	},
	{
		name: "standard documents",
		docs: []doc.Metadata{
			{
				ID: []byte("831992"),
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
				ID: []byte("080392"),
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
		},
	},
	{
		name: "node exporter metrics",
		docs: util.MustReadDocs("../../../../../util/testdata/node_exporter.json", 2000),
	},
}

func TestStoredFieldsData(t *testing.T) {
	w := NewDataWriter(nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				buf     = new(bytes.Buffer)
				offsets = make([]int, 0)
				idx     int
			)
			w.Reset(buf)

			for i := range test.docs {
				n, err := w.Write(test.docs[i])
				require.NoError(t, err)
				offsets = append(offsets, idx)
				idx += n
			}

			r := NewDataReader(buf.Bytes())
			for i := range test.docs {
				actual, err := r.Read(uint64(offsets[i]))
				require.NoError(t, err)
				require.True(t, actual.Equal(test.docs[i]))
			}
		})
	}
}

func TestEncodedDataReader(t *testing.T) {
	w := NewDataWriter(nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				buf     = new(bytes.Buffer)
				offsets = make([]int, 0)
				idx     int
			)
			w.Reset(buf)

			for i := range test.docs {
				n, err := w.Write(test.docs[i])
				require.NoError(t, err)
				offsets = append(offsets, idx)
				idx += n
			}

			dataReader := NewEncodedDataReader(buf.Bytes())
			docReader := NewEncodedDocumentReader()
			for i := range test.docs {
				encoded, err := dataReader.Read(uint64(offsets[i]))
				require.NoError(t, err)

				actual, err := docReader.Read(encoded)
				require.NoError(t, err)
				require.True(t, actual.Equal(test.docs[i]))
			}
		})
	}
}
