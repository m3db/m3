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

package idkey

import (
	"testing"
	"unsafe"

	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/mauricelam/genny/generic"
	"github.com/stretchr/testify/require"
)

// nolint: structcheck
func TestMapWithPooling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := ident.StringID("foo")
	value := generic.Type("a")

	pool := pool.NewMockBytesPool(ctrl)

	m := NewMap(MapOptions{KeyCopyPool: pool})

	mockPooledSlice := make([]byte, 0, 3)
	pool.EXPECT().Get(len(key.Bytes())).Return(mockPooledSlice)
	m.Set(key, value)
	require.Equal(t, 1, m.Len())

	// Now ensure that the key is from the pool and not our original key
	for _, entry := range m.Iter() {
		type slice struct {
			array unsafe.Pointer
			len   int
			cap   int
		}

		keyBytes := []byte(entry.Key().(ident.BytesID))

		rawPooledSlice := (*slice)(unsafe.Pointer(&mockPooledSlice))
		rawKeySlice := (*slice)(unsafe.Pointer(&keyBytes))

		require.True(t, rawPooledSlice.array == rawKeySlice.array)
	}

	// Now delete the key to simulate returning to pool
	pool.EXPECT().Put(mockPooledSlice[:3])
	m.Delete(key)
	require.Equal(t, 0, m.Len())
}
