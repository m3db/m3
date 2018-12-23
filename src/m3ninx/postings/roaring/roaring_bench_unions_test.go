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

package roaring

import (
	"math/rand"
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

const (
	numPls              = 10
	numElemsPer         = 10000
	numTotalElems       = numElemsPer * numPls
	seed          int64 = 123456789
)

func newRandPostingsLists(numPostingsLists, numElemsPer int) []*roaring.Bitmap {
	rng := rand.New(rand.NewSource(seed))
	elems := make([]uint64, 0, numElemsPer)
	pls := make([]*roaring.Bitmap, 0, numPostingsLists)
	for j := 0; j < numPostingsLists; j++ {
		elems = elems[:0]
		for i := 0; i < numElemsPer; i++ {
			elems = append(elems, rng.Uint64())
		}
		pls = append(pls, roaring.NewBitmap(elems...))
	}
	return pls
}

func newSampledPostingsLists(numPostingsLists, numTotalElements int) []*roaring.Bitmap {
	elems := make([][]uint64, numPostingsLists)
	for i := 0; i < numTotalElements; i++ {
		idx := i % numPostingsLists
		elems[idx] = append(elems[idx], uint64(i))
	}
	pls := make([]*roaring.Bitmap, 0, numPostingsLists)
	for _, elem := range elems {
		pls = append(pls, roaring.NewBitmap(elem...))
	}
	return pls
}

func newSampledPostingsListsPilosa(numPostingsLists, numTotalElements int) []*roaring.Bitmap {
	elems := make([][]uint64, numPostingsLists)
	for i := 0; i < numTotalElements; i++ {
		idx := i % numPostingsLists
		elems[idx] = append(elems[idx], uint64(i))
	}
	pls := make([]*roaring.Bitmap, 0, numPostingsLists)
	for _, elem := range elems {
		pls = append(pls, roaring.NewBitmap(elem...))
	}
	return pls
}

func BenchmarkUnionRandPlsFastOr(b *testing.B) {
	pls := newRandPostingsLists(numPls, numElemsPer)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.NewBitmap().UnionInPlace(pls...)
	}
}

func BenchmarkUnionSampledPlsFastOr(b *testing.B) {
	pls := newSampledPostingsLists(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.NewBitmap().UnionInPlace(pls...)
	}
}
