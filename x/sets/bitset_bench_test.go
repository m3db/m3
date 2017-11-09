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

import "testing"
import "bytes"
import "encoding/binary"

// go test -bench=BenchmarkBitSet
// see http://lemire.me/blog/2016/09/22/swift-versus-java-the-bitset-performance-test/
func BenchmarkBitSetLemireCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		set := NewBitSet(0) // we force dynamic memory allocation
		for v := uint(0); v <= 100000000; v += 100 {
			set.Set(v)
		}
	}
}

func BenchmarkBitSetBinaryWrite(b *testing.B) {
	set := NewBitSet(10000)
	buf := bytes.NewBuffer(make([]byte, len(set.values)*8))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		binary.Write(buf, binary.LittleEndian, set.values)
	}
}

func BenchmarkBitSetPutUint64(b *testing.B) {
	set := NewBitSet(10000)
	buf := bytes.NewBuffer(make([]byte, len(set.values)*8))
	b.ResetTimer()

	var single [8]byte
	for i := 0; i < b.N; i++ {
		buf.Reset()
		for _, x := range set.values {
			binary.LittleEndian.PutUint64(single[:], x)
			_, err := buf.Write(single[:])
			if err != nil {
				b.Fatalf(err.Error())
			}
		}
	}
}
