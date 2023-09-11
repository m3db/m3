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
	"github.com/cespare/xxhash/v2"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
)

type xxHasher struct{}

// NewXXHasher returns an IndexEntryHasher utilizing xxHash hashing.
func NewXXHasher() IndexEntryHasher {
	return xxHasher{}
}

func (x xxHasher) HashIndexEntry(
	id ident.BytesID,
	encodedTags ts.EncodedTags,
	dataChecksum int64,
) int64 {
	hash := uint64(7)
	hash = 31*hash + xxhash.Sum64(id)
	hash = 31*hash + xxhash.Sum64(encodedTags)
	hash = 31*hash + uint64(dataChecksum)

	return int64(hash)
}
