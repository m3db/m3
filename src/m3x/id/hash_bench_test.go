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

package id

import (
	"math/rand"
	"testing"
	"time"
)

const (
	testShortIDSize  = 8
	testMediumIDSize = 200
	testLongIDSize   = 8192
)

var (
	testShortID, testMediumID, testLongID []byte
)

func benchmarkMD5ShortID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		HashFn(testShortID)
	}
}

func benchmarkMD5MediumID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		HashFn(testMediumID)
	}
}

func benchmarkMD5LongID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		HashFn(testLongID)
	}
}

func benchmarkMurmur3Hash128ShortID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Murmur3Hash128(testShortID)
	}
}

func benchmarkMurmur3Hash128MediumID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Murmur3Hash128(testMediumID)
	}
}

func benchmarkMurmur3Hash128LongID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Murmur3Hash128(testLongID)
	}
}

func BenchmarkHashingFunctions(b *testing.B) {
	setup()

	benchmarks := []struct {
		description string
		fn          func(b *testing.B)
	}{
		{description: "Benchmark MD5 for short id", fn: benchmarkMD5ShortID},
		{description: "Benchmark MD5 for medium id", fn: benchmarkMD5MediumID},
		{description: "Benchmark MD5 for long id", fn: benchmarkMD5LongID},
		{description: "Benchmark 128-bit murmur3 hash for short id", fn: benchmarkMurmur3Hash128ShortID},
		{description: "Benchmark 128-bit murmur3 hash for medium id", fn: benchmarkMurmur3Hash128MediumID},
		{description: "Benchmark 128-bit murmur3 for long id", fn: benchmarkMurmur3Hash128LongID},
	}
	for _, bench := range benchmarks {
		b.Run(bench.description, bench.fn)
	}
}

func setup() {
	s := rand.New(rand.NewSource(time.Now().UnixNano()))
	testShortID = generateTestID(s, testShortIDSize)
	testMediumID = generateTestID(s, testMediumIDSize)
	testLongID = generateTestID(s, testLongIDSize)
}

func generateTestID(s *rand.Rand, size int) []byte {
	id := make([]byte, size)
	for i := 0; i < size; i++ {
		id[i] = byte(s.Int63() % 256)
	}
	return id
}
