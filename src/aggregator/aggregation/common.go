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

package aggregation

import (
	"math"

	"github.com/m3db/m3/src/metrics/aggregation"
)

func stdev(count int64, sumSq, sum float64) float64 {
	div := count * (count - 1)
	if div == 0 {
		return 0.0
	}
	num := float64(count)*sumSq - sum*sum
	return math.Sqrt(num / float64(div))
}

func isExpensive(aggTypes aggregation.Types) bool {
	for _, aggType := range aggTypes {
		if aggType == aggregation.SumSq || aggType == aggregation.Stdev {
			return true
		}
	}
	return false
}

func maybeReplaceAnnotation(current, new []byte) []byte {
	if len(new) == 0 {
		return current
	}
	// Keep the last annotation which was set. Reuse any previous allocation while taking a copy.
	result := current[:0]
	if cap(result) < len(new) {
		// Twice as long in case another one comes in
		// and we could avoid realloc as long as less than this first alloc.
		result = make([]byte, 0, 2*len(new))
	}
	return append(result, new...)
}
