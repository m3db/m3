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

package byteskey

import (
	"bytes"

	"github.com/m3db/m3x/pool"

	"github.com/cespare/xxhash"
	"github.com/mauricelam/genny/generic"
)

// MapValue is the generic type that needs to be specified when generating.
type MapValue generic.Type

// MapOptions provides options used when created the map.
type MapOptions struct {
	InitialSize int
	KeyCopyPool pool.BytesPool
}

// NewMap returns a new byte keyed map.
func NewMap(opts MapOptions) *Map {
	var (
		copyFn     CopyFn
		finalizeFn FinalizeFn
	)
	if pool := opts.KeyCopyPool; pool == nil {
		copyFn = func(k []byte) []byte {
			return append([]byte(nil), k...)
		}
	} else {
		copyFn = func(k []byte) []byte {
			keyLen := len(k)
			pooled := pool.Get(keyLen)[:keyLen]
			copy(pooled, k)
			return pooled
		}
		finalizeFn = func(k []byte) {
			pool.Put(k)
		}
	}
	return mapAlloc(mapOptions{
		hash: func(k []byte) MapHash {
			return MapHash(xxhash.Sum64(k))
		},
		equals:      bytes.Equal,
		copy:        copyFn,
		finalize:    finalizeFn,
		initialSize: opts.InitialSize,
	})
}
