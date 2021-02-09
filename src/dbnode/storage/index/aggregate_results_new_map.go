// Copyright (c) 2019 Uber Technologies, Inc.
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

package index

import (
	"bytes"

	"github.com/m3db/m3/src/x/pool"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultInitialAggregatedResultsMapSize = 10
)

func newAggregateResultsMap(bytesPool pool.BytesPool) *AggregateResultsMap {
	return _AggregateResultsMapAlloc(_AggregateResultsMapOptions{
		hash: func(k []byte) AggregateResultsMapHash {
			return AggregateResultsMapHash(xxhash.Sum64(k))
		},
		equals: func(x, y []byte) bool {
			return bytes.Equal(x, y)
		},
		copy: func(k []byte) []byte {
			cloned := bytesPool.Get(len(k))
			copy(cloned, k)
			return cloned
		},
		finalize: func(k []byte) {
			bytesPool.Put(k)
		},
		initialSize: defaultInitialAggregatedResultsMapSize,
	})
}
