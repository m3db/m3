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

package ident

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/m3db/m3x/checked"
)

// BinaryID constructs a new ID based on a binary value.
func BinaryID(v checked.Bytes) ID {
	v.IncRef()
	return &id{data: v}
}

// StringID constructs a new ID based on a string value.
func StringID(str string) ID {
	v := checked.NewBytes([]byte(str), nil)
	v.IncRef()
	return &id{data: v}
}

type id struct {
	data checked.Bytes
	hash Hash
	pool IdentifierPool
	flag int32
}

// Data returns the binary value of an ID.
func (v *id) Data() checked.Bytes {
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
			v.hash = HashFn(v.data.Get())
			atomic.StoreInt32(&v.flag, int32(computed))
			return v.hash
		}
	default:
		panic(fmt.Sprintf("unexpected hash state: %v", flag))
	}

	// If the hash is being computed, compute the hash in place and don't
	// wait.
	return HashFn(v.data.Get())
}

func (v *id) Equal(value ID) bool {
	currNoData := v == nil || v.Data() == nil || v.Data().Len() == 0
	otherNoData := value == nil || value.Data() == nil || value.Data().Len() == 0
	if currNoData && otherNoData {
		return true
	}
	if currNoData || otherNoData {
		return false
	}
	return bytes.Equal(v.Data().Get(), value.Data().Get())
}

func (v *id) Finalize() {
	v.data.DecRef()
	v.data.Finalize()
	v.data = nil
	v.hash = null

	if v.pool == nil {
		return
	}

	v.pool.Put(v)
}

func (v *id) Reset() {
	v.data, v.hash, v.flag = nil, null, int32(invalid)
}

func (v *id) String() string {
	if v.data == nil {
		return ""
	}
	return string(v.data.Get())
}
