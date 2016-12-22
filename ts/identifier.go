// Copyright (c) 2016 Uber Technologies, Inc.
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

package ts

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"sync/atomic"
)

// BinaryID constructs a new ID based on a binary value.
// WARNING: Does not copy the underlying data, do not use
// when cloning a pooled ID object.
func BinaryID(v []byte) ID {
	return &id{data: v}
}

// StringID constructs a new ID based on a string value.
func StringID(v string) ID {
	return &id{data: []byte(v)}
}

// HashFn is the default hashing implementation for IDs.
func HashFn(data []byte) Hash {
	return md5.Sum(data)
}

type id struct {
	data []byte
	hash Hash
	pool IdentifierPool
	flag int32
}

// Data returns the binary value of an ID.
func (v *id) Data() []byte {
	return v.data
}

type hashFlag int32

const (
	invalid hashFlag = iota
	pending
	computed
)

var null = Hash{}

// Hash calculates and returns the hash of an ID.
func (v *id) Hash() Hash {
	switch flag := hashFlag(atomic.LoadInt32(&v.flag)); flag {
	case computed:
		// If the hash has been computed, return the previously cached value.
		return v.hash
	case pending:
		break
	case invalid:
		// If the hash is not computed, and this goroutine gains exclusive
		// access to the hash field, compute and cache it.
		if atomic.CompareAndSwapInt32(&v.flag, int32(invalid), int32(pending)) {
			v.hash = HashFn(v.data)
			atomic.StoreInt32(&v.flag, int32(computed))
			return v.hash
		}
	default:
		panic(fmt.Sprintf("unexpected hash state: %v", flag))
	}

	// If the hash is being computed, compute the hash in place and don't
	// wait.
	return HashFn(v.data)
}

func (v *id) Equal(value ID) bool {
	return bytes.Equal(v.Data(), value.Data())
}

func (v *id) Finalize() {
	if v.pool == nil {
		return
	}

	v.pool.Put(v)
}

func (v *id) Reset() {
	v.data, v.hash, v.flag = nil, null, int32(invalid)
}

func (v *id) String() string {
	return string(v.data)
}
