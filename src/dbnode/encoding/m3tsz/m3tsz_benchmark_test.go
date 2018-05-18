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
	"math/big"
	"strconv"
	"strings"
	"testing"
)

const (
	smallDpFloat = 123.456
	largeDpFloat = 123.4567890123
	intFloat     = 123.0
)

func BenchmarkMathPow(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = smallDpFloat * math.Pow10(1)
	}
}

func BenchmarkManualMult(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = smallDpFloat * 10.0
	}
}

func BenchmarkSliceLookup(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_ = largeDpFloat * multipliers[6]
	}
}

func BenchmarkFuncPointer(b *testing.B) {
	benchPointer(b, funcNormal)
}

func BenchmarkBoolCheckTrue(b *testing.B) {
	benchBoolCheck(b, true)
}

func BenchmarkBoolCheckFalse(b *testing.B) {
	benchBoolCheck(b, false)
}

func BenchmarkMathModf(b *testing.B) {
	for n := 0; n < b.N; n++ {
		math.Modf(largeDpFloat)
	}
}

func BenchmarkMathNextafter(b *testing.B) {
	for n := 0; n < b.N; n++ {
		math.Nextafter(largeDpFloat, 2)
	}
}

func BenchmarkFormatFloat(b *testing.B) {
	for n := 0; n < b.N; n++ {
		strconv.FormatFloat(largeDpFloat, 'f', -1, 64)
	}
}

func BenchmarkMathConversionInt(b *testing.B) {
	benchMathConversion(b, intFloat)
}

func BenchmarkNoCheckConversionInt(b *testing.B) {
	benchNoCheckConversion(b, intFloat)
}

func BenchmarkStringConversionInt(b *testing.B) {
	benchStringConversion(b, intFloat)
}

func BenchmarkMathConversionSmall(b *testing.B) {
	benchMathConversion(b, smallDpFloat)
}

func BenchmarkNoCheckConversionSmall(b *testing.B) {
	benchNoCheckConversion(b, smallDpFloat)
}

func BenchmarkStringConversionSmall(b *testing.B) {
	benchStringConversion(b, smallDpFloat)
}

func BenchmarkMathConversionLarge(b *testing.B) {
	benchMathConversion(b, largeDpFloat)
}

func BenchmarkNoCheckConversionLarge(b *testing.B) {
	benchNoCheckConversion(b, largeDpFloat)
}

func BenchmarkStringConversionLarge(b *testing.B) {
	benchStringConversion(b, largeDpFloat)
}

func BenchmarkMathConversionLargeConst(b *testing.B) {
	benchMathConstConversion(b, largeDpFloat)
}

func BenchmarkStringConversionLargeConst(b *testing.B) {
	benchStringConstConversion(b, largeDpFloat)
}

func benchMathConversion(b *testing.B, val float64) {
	for n := 0; n < b.N; n++ {
		convertToIntFloat(val, 0)
	}
}

func benchMathConstConversion(b *testing.B, val float64) {
	var dec uint8
	for n := 0; n < b.N; n++ {
		_, dec, _, _ = convertToIntFloat(val, dec)
	}
}

func benchStringConstConversion(b *testing.B, val float64) {
	var dec uint8
	for n := 0; n < b.N; n++ {
		_, dec, _ = convertToIntString(val, dec)
	}
}

func benchStringConversion(b *testing.B, val float64) {
	for n := 0; n < b.N; n++ {
		convertToIntString(val, 0)
	}
}

func benchNoCheckConversion(b *testing.B, val float64) {
	for n := 0; n < b.N; n++ {
		convertToIntFloatIntNoCheck(val, 0)
	}
}

func benchPointer(b *testing.B, f writeFunc) {
	for n := 0; n < b.N; n++ {
		f(largeDpFloat)
	}
}

func benchBoolCheck(b *testing.B, enabled bool) {
	for n := 0; n < b.N; n++ {
		if enabled {
			funcNormal(largeDpFloat)
		} else {
			funcOpt(largeDpFloat)
		}
	}
}

type writeFunc func(v float64)

func funcNormal(v float64) {}
func funcOpt(v float64)    {}

func convertToIntFloatIntNoCheck(v float64, curMaxMult uint8) (float64, uint8, bool) {
	val := v * math.Pow10(int(curMaxMult))
	sign := 1.0
	if v < 0 {
		sign = -1.0
		val = val * -1.0
	}

	for mult := curMaxMult; mult <= maxMult && val < maxOptInt; mult++ {
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

func convertToIntString(v float64, curDec uint8) (float64, uint8, bool) {
	val := big.NewFloat(v)

	if curDec == 0 {
		i, acc := val.Int64()
		if acc == big.Exact {
			return float64(i), 0, false
		}
	}

	s := strconv.FormatFloat(v, 'f', -1, 64)
	dec := uint8(len(s)-strings.Index(s, ".")) - 1

	if dec < curDec {
		dec = curDec
	} else if dec > maxMult {
		dec = maxMult
	}

	val.Mul(val, big.NewFloat(math.Pow10(int(dec))))
	i, _ := val.Int64()
	if i != math.MaxInt64 && i != math.MinInt64 {
		mv, _ := val.Float64()
		i = roundFloat(mv, i)
		return float64(i), dec, false
	}

	return v, 0, true
}

func roundFloat(v float64, i int64) int64 {
	_, r := math.Modf(v)
	if r < 0.5 {
		return i
	}

	return i + 1
}
