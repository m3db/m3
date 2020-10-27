// Copyright (c) 2020 Uber Technologies, Inc.
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

package roaring

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/m3dbx/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

const (
	readOnlySeed int64 = 123456789
)

func TestReadOnlyBitmap(t *testing.T) {
	buff := bytes.NewBuffer(nil)

	rng := rand.New(rand.NewSource(seed))

	each := 8
	tests := []struct {
		attempts    int
		insertCount int
		insertRange int
	}{
		// 64 inserts
		{
			insertCount: 64,
			insertRange: 64,
		},
		{
			insertCount: 64,
			insertRange: 128,
		},
		{
			insertCount: 64,
			insertRange: 256,
		},
		// 4096 inserts
		{
			insertCount: 4096,
			insertRange: 4096,
		},
		{
			insertCount: 4096,
			insertRange: 8192,
		},
		{
			insertCount: 4096,
			insertRange: 16384,
		},
		// 65536 inserts
		{
			insertCount: 65536,
			insertRange: 65536,
		},
		{
			insertCount: 65536,
			insertRange: 131072,
		},
		{
			insertCount: 4096,
			insertRange: 262144,
		},
	}

	b := roaring.NewBitmapWithDefaultPooling(2 << 15) // 2^16 containers max, will stay within [0,2^32)
	for _, test := range tests {
		for i := 0; i < each; i++ {
			t.Run(fmt.Sprintf("attempt=%d, test=+%v", i, test), func(t *testing.T) {
				b.Reset()
				max := uint64(rng.Int63n(int64(test.insertRange)))
				for j := 0; j < test.insertCount; j++ {
					value := rng.Uint64() % max
					b.DirectAdd(value)
				}

				list := NewPostingsListFromBitmap(b)

				// Note: Do not reuse buffer before done with
				// read only map that is backed by the bytes from the
				// bufer.
				buff.Reset()
				_, err := b.WriteTo(buff)
				require.NoError(t, err)

				readOnly, err := NewReadOnlyBitmap(buff.Bytes())
				require.NoError(t, err)

				// Check for equality.
				require.True(t, readOnly.Equal(list))

				// Check for contains.
				iter := list.Iterator()
				for iter.Next() {
					curr := iter.Current()
					require.True(t, readOnly.Contains(curr))
				}
				require.NoError(t, iter.Err())
				require.NoError(t, iter.Close())
			})
		}
	}
}
