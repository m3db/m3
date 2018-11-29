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

	"github.com/RoaringBitmap/roaring"
	"github.com/pilosa/pilosa"
)

// ❯ go test -bench=BenchmarkUnion ./src/m3ninx/postings/roaring
// goos: darwin
// goarch: amd64
// pkg: github.com/m3db/m3/src/m3ninx/postings/roaring
// BenchmarkUnionRandPlsFastOr-4         	       2	 599631022 ns/op	246896384 B/op	  213828 allocs/op
// BenchmarkUnionRandPlsHeapOr-4         	      20	  57474700 ns/op	31526924 B/op	  474318 allocs/op
// BenchmarkUnionRandPlsParOr-4          	      10	 200720468 ns/op	244939915 B/op	  146408 allocs/op
// BenchmarkUnionRandPlsParHeapOr-4      	       5	 265391848 ns/op	467047537 B/op	  269679 allocs/op
// BenchmarkUnionSampledPlsFastOr-4      	   20000	     73632 ns/op	   24904 B/op	      13 allocs/op
// BenchmarkUnionSampledPlsHeapOr-4      	   10000	    132481 ns/op	  150088 B/op	     104 allocs/op
// BenchmarkUnionSampledPlsParOr-4       	   20000	     80480 ns/op	   25489 B/op	      25 allocs/op
// BenchmarkUnionSampledPlsParHeapOr-4   	   20000	     86494 ns/op	   33488 B/op	      48 allocs/op
// PASS
// ok  	github.com/m3db/m3/src/m3ninx/postings/roaring	19.043s

const (
	numPls              = 10
	numElemsPer         = 10000
	numTotalElems       = numElemsPer * numPls
	seed          int64 = 123456789
)

func newRandPostingsLists(numPostingsLists, numElemsPer int) []*roaring.Bitmap {
	rng := rand.New(rand.NewSource(seed))
	elems := make([]uint32, 0, numElemsPer)
	pls := make([]*roaring.Bitmap, 0, numPostingsLists)
	for j := 0; j < numPostingsLists; j++ {
		elems = elems[:0]
		for i := 0; i < numElemsPer; i++ {
			elems = append(elems, rng.Uint32())
		}
		pls = append(pls, roaring.BitmapOf(elems...))
	}
	return pls
}

func newSampledPostingsLists(numPostingsLists, numTotalElements int) []*roaring.Bitmap {
	elems := make([][]uint32, numPostingsLists)
	for i := 0; i < numTotalElements; i++ {
		idx := i % numPostingsLists
		elems[idx] = append(elems[idx], uint32(i))
	}
	pls := make([]*roaring.Bitmap, 0, numPostingsLists)
	for _, elem := range elems {
		pls = append(pls, roaring.BitmapOf(elem...))
	}
	return pls
}

func newSampledPostingsListsPilosa(numPostingsLists, numTotalElements int) []*pilosa.Bitmap {
	elems := make([][]uint64, numPostingsLists)
	for i := 0; i < numTotalElements; i++ {
		idx := i % numPostingsLists
		elems[idx] = append(elems[idx], uint64(i))
	}
	pls := make([]*pilosa.Bitmap, 0, numPostingsLists)
	for _, elem := range elems {
		pls = append(pls, pilosa.NewBitmap(elem...))
	}
	return pls
}

func BenchmarkUnionRandPlsFastOr(b *testing.B) {
	pls := newRandPostingsLists(numPls, numElemsPer)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.FastOr(pls...)
	}
}

func BenchmarkUnionRandPlsHeapOr(b *testing.B) {
	pls := newRandPostingsLists(numPls, numElemsPer)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.HeapOr(pls...)
	}
}

func BenchmarkUnionRandPlsParOr(b *testing.B) {
	pls := newRandPostingsLists(numPls, numElemsPer)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.ParOr(0, pls...)
	}
}

func BenchmarkUnionRandPlsParHeapOr(b *testing.B) {
	pls := newRandPostingsLists(numPls, numElemsPer)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.ParHeapOr(0, pls...)
	}
}

func BenchmarkUnionSampledPlsFastOr(b *testing.B) {
	pls := newSampledPostingsLists(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.FastOr(pls...)
	}
}

func BenchmarkUnionSampledPlsFastOrPilosa(b *testing.B) {
	pls := newSampledPostingsListsPilosa(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pilosa.NewBitmap().UnionInPlace()
		unioned := pls[0]
		for _, pl := range pls[1:] {
			unioned = unioned.Union(pl)
		}
	}
}

func BenchmarkUnionSampledPlsHeapOr(b *testing.B) {
	pls := newSampledPostingsLists(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.HeapOr(pls...)
	}
}

func BenchmarkUnionSampledPlsParOr(b *testing.B) {
	pls := newSampledPostingsLists(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.ParOr(0, pls...)
	}
}

func BenchmarkUnionSampledPlsParHeapOr(b *testing.B) {
	pls := newSampledPostingsLists(numPls, numTotalElems)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roaring.ParHeapOr(0, pls...)
	}
}
