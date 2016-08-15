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

package m3tsz

import (
	"math"
)

const (
	opcodeZeroValueXOR        = 0x0
	opcodeContainedValueXOR   = 0x2
	opcodeUncontainedValueXOR = 0x3

	opcodeNoUpdate   = 0x0
	opcodeUpdate     = 0x1
	opcodeUpdateSig  = 0x1
	opcodeUpdateMult = 0x1
	opcodeNegative   = 0x1
	opcodePositive   = 0x0
	opcodeRepeat     = 0x1
	opcodeNoRepeat   = 0x0
	opcodeFloatMode  = 0x1
	opcodeIntMode    = 0x0
	opcodeZeroSig    = 0x0
	opcodeNonZeroSig = 0x1

	maxMult     = uint8(6)
	numMultBits = 3
	numSigBits  = 6
)

var maxInt = float64(math.MaxInt64)

// convertToIntFloat takes a float64 val and the current max multiplier
// and returns a uint64 representation of the value, along with
// an updated multiplier and a bool indicating whether the uint64
// is floatBits
func convertToIntFloat(v float64, curMaxMult uint8) (float64, uint8, bool) {
	if curMaxMult == 0 {
		// Quick check for vals that are already ints
		i, r := math.Modf(v)
		if r == 0 {
			return i, 0, false
		}
	}

	val := v * math.Pow10(int(curMaxMult))
	sign := 1.0
	if v < 0 {
		sign = -1.0
		val = val * -1.0
	}

	for mult := curMaxMult; mult <= maxMult && val < maxInt; mult++ {
		i, r := math.Modf(val)
		if r == 0 {
			return sign * i, mult, false
		} else if r < 0.5 {
			// Round down and check
			if math.Nextafter(val, 0) <= i {
				return sign * i, mult, false
			}
		} else {
			// Round up and check
			next := i + 1
			if math.Nextafter(val, next) >= next {
				return sign * next, mult, false
			}
		}
		val = val * 10.0
	}

	return v, 0, true
}

func convertFromIntFloat(val float64, mult uint8) float64 {
	if mult == 0 {
		return val
	}

	return val / math.Pow10(int(mult))
}
