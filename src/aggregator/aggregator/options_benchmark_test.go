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

package aggregator

import "testing"

func BenchmarkSuffixSlice(b *testing.B) {
	opts := NewOptions().AggregationTypesOptions()
	var a []byte

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		aggTypes := opts.DefaultTimerAggregationTypes()
		suffixes := opts.DefaultTimerAggregationTypeStrings()
		for i := range aggTypes {
			a = suffixes[i]
		}
	}
	b.StopTimer()
	// For compiler's sake.
	a[0] = byte('b')
}

func BenchmarkSuffixFn(b *testing.B) {
	opts := NewOptions().AggregationTypesOptions()
	var a []byte

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		aggTypes := opts.DefaultTimerAggregationTypes()
		for _, aggType := range aggTypes {
			a = opts.TypeStringForTimer(aggType)
		}
	}
	b.StopTimer()
	// For compiler's sake.
	a[0] = byte('b')
}
