// Copyright (c) 2017 Uber Technologies, Inc.
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

package xsets

import (
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
)

func TestBloomFilterEstimate(t *testing.T) {
	n := uint(216553)
	p := 0.01
	m, k := BloomFilterEstimate(n, p)
	assert.Equal(t, uint(2075674), m)
	assert.Equal(t, uint(7), k)
}

func TestBloomFilterHashesZeroAlloc(t *testing.T) {
	d := make([]byte, 32)
	for i := range d {
		d[i] = byte(i)
	}

	hash := murmur3.New128()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	startAllocs := stats.Mallocs

	n := 4096
	for i := 0; i < n; i++ {
		v := bloomFilterHashes(hash, d)
		for j := 0; j < len(v); j++ {
			binary.LittleEndian.PutUint64(d[j*8:(j+1)*8], v[j])
		}
	}

	runtime.ReadMemStats(&stats)
	endAllocs := stats.Mallocs
	avg := float64(endAllocs-startAllocs) / float64(n)
	// sometimes a background goroutine allocs even though
	// tests should be done exclusively and sequentially..
	assert.True(t, avg < 0.01)
}

// BenchmarkBloomFilterHashes should always be zero alloc or else
// we're going to have a "bad time" when it comes adding millions
// of entries to the bloom filter.  The test TestBlomFilterHashesZeroAlloc
// above also verifies that zero allocations occurs.
//
// Test with the following to track down leaks/allocs:
// go test -v -bench BenchmarkBloomFilterHashes -benchmem -gcflags -m
func BenchmarkBloomFilterHashes(b *testing.B) {
	d := make([]byte, 32)
	for i := range d {
		d[i] = byte(i)
	}
	hash := murmur3.New128()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v := bloomFilterHashes(hash, d)
		for j := 0; j < len(v); j++ {
			binary.LittleEndian.PutUint64(d[j*8:(j+1)*8], v[j])
		}
	}
}
