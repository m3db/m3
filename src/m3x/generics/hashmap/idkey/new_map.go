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
	"bytes"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/cespare/xxhash"
	"github.com/cheekybits/genny/generic"
)

// Value is the generic type that needs to be specified when generating.
type Value generic.Type

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
			return id(append([]byte(nil), k.Bytes()...))
		}
	} else {
		copyFn = func(k ident.ID) ident.ID {
			bytes := k.Bytes()
			keyLen := len(bytes)
			pooled := pool.Get(keyLen)[:keyLen]
			copy(pooled, bytes)
			return id(pooled)
		}
		finalizeFn = func(k ident.ID) {
			if slice, ok := k.(id); ok {
				pool.Put(slice)
			}
		}
	}
	return newMap(mapOptions{
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

// id is a small utility type to avoid the heavy-ness of the true ident.ID
// implementation when copying keys just for the use of looking up keys in
// the map internally.
type id []byte

// var declaration to ensure package type id implements ident.ID
var _ ident.ID = id(nil)

func (v id) Data() checked.Bytes {
	// Data is not called by the generated hashmap code, hence we don't
	// implement this and no callers will call this as the generated code
	// is the only user of this type.
	panic("not implemented")
}

func (v id) Bytes() []byte {
	return v
}

func (v id) String() string {
	return string(v)
}

func (v id) Equal(value ident.ID) bool {
	return bytes.Equal(value.Bytes(), v)
}

func (v id) Reset() {
	for i := range v {
		v[i] = byte(0)
	}
}

func (v id) Finalize() {
}
