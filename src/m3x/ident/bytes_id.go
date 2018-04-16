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

package ident

import (
	"bytes"

	"github.com/m3db/m3x/checked"
)

// BytesID is a small utility type to avoid the heavy weight of a true ID
// implementation when using in high throughput places like keys in a map.
type BytesID []byte

// var declaration to ensure package type BytesID implements ID
var _ ID = BytesID(nil)

// Data returns the bytes ID in a checked bytes container.
func (v BytesID) Data() checked.Bytes {
	// Data is not called by the generated hashmap code, hence we don't
	// implement this and no callers will call this as the generated code
	// is the only user of this type.
	return checked.NewBytes(v, nil)
}

// Bytes returns the underlying byte slice of the bytes ID.
func (v BytesID) Bytes() []byte {
	return v
}

// String returns the bytes ID as a string.
func (v BytesID) String() string {
	return string(v)
}

// Equal returns whether the bytes ID is equal to a given ID.
func (v BytesID) Equal(value ID) bool {
	return bytes.Equal(value.Bytes(), v)
}

// Reset zeroes the bytes ID byte values.
func (v BytesID) Reset() {
	for i := range v {
		v[i] = byte(0)
	}
}

// Finalize is a no-op for a bytes ID as it has no associated pool.
func (v BytesID) Finalize() {
}
