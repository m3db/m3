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
	"github.com/m3db/m3x/ident"
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
		copyFn = func(k ident.ID) ident.ID {
			return ident.BytesID(append([]byte(nil), k.Bytes()...))
		}
	} else {
		copyFn = func(k ident.ID) ident.ID {
			bytes := k.Bytes()
			keyLen := len(bytes)
			pooled := pool.Get(keyLen)[:keyLen]
			copy(pooled, bytes)
			return ident.BytesID(pooled)
		}
		finalizeFn = func(k ident.ID) {
			if slice, ok := k.(ident.BytesID); ok {
				pool.Put(slice)
			}
		}
	}
	return mapAlloc(mapOptions{
		hash: func(id ident.ID) MapHash {
			return MapHash(xxhash.Sum64(id.Bytes()))
		},
		equals: func(x, y ident.ID) bool {
			return x.Equal(y)
		},
		copy:        copyFn,
		finalize:    finalizeFn,
		initialSize: opts.InitialSize,
	})
}
