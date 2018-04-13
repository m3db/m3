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

package storage

import (
	"bytes"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
)

type mapHash uint64
type hashFn func(ident.ID) mapHash
type equalsFn func(ident.ID, ident.ID) bool
type copyFn func(ident.ID) ident.ID
type finalizeFn func(ident.ID)

// mapOptions is a set of options used when creating an identifier map, it is kept
// private so that implementers of the generated map can specify their own options
// that partially fulfill these options.
type mapOptions struct {
	// hash is the hash function to execute when hashing a key.
	hash hashFn
	// equals is the equals key function to execute when detecting equality.
	equals equalsFn
	// copy is the copy key function to execute when copying the key.
	copy copyFn
	// finalize is the finalize key function to execute when finished with a
	// key, this is optional to specify.
	finalize finalizeFn
	// initialSize is the initial size for the map, use zero to use Go's std map
	// initial size and consequently is optional to specify.
	initialSize int
}

// nolint:structcheck
type mapKeyOptions struct {
	copyKey     bool
	finalizeKey bool
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
