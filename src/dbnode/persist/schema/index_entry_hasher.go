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

package schema

import (
	"hash"
	"hash/adler32"
	"sync"
)

type adlerHasher struct {
	sync.Mutex
	hash hash.Hash32
}

// NewAdlerHasher returns an IndexEntryHasher utilizing adler32 hashing.
func NewAdlerHasher() IndexEntryHasher {
	return &adlerHasher{hash: adler32.New()}
}

func (h *adlerHasher) HashIndexEntry(e IndexEntry) int64 {
	h.Lock()
	h.hash.Reset()
	h.hash.Write(e.ID)
	h.hash.Write(e.EncodedTags)
	// TODO: investigate performance of this; also may be neecessary to add
	// other fields to this hash. Also subrtacting a computed value from the hash
	// may lead to more collision prone values.
	hash := int64(h.hash.Sum32()) - e.DataChecksum
	h.Unlock()
	return hash
}
