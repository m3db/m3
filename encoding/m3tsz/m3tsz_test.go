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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCountConversions(t *testing.T) {
	testIntConversions(t, 1000, 0, 0)
	testIntConversions(t, 1000, 1, 0)
	testIntConversions(t, 1000, 2, 0)
	testIntConversions(t, 1000, 10, 0)
	testIntConversions(t, 1000, 18, 0)
}

func TestTimerConversions(t *testing.T) {
	testIntConversions(t, 1000, 0, 6)
	testIntConversions(t, 1000, 1, 6)
	testIntConversions(t, 1000, 3, 6)
	testIntConversions(t, 1000, 5, 6)
	testIntConversions(t, 1000, 7, 6)
}

func TestSmallGaugeConversions(t *testing.T) {
	testIntConversions(t, 1000, 0, 1)
	testIntConversions(t, 1000, 0, 3)
	testIntConversions(t, 1000, 1, 3)
	testIntConversions(t, 1000, 3, 3)
	testIntConversions(t, 1000, 5, 3)
	testIntConversions(t, 1000, 7, 3)
}

func TestPreciseGaugeConversions(t *testing.T) {
	testFloatConversions(t, 1000, 0, 16)
	testFloatConversions(t, 1000, 1, 16)
	testFloatConversions(t, 1000, 5, 16)
}

func TestLargeGaugeConversions(t *testing.T) {
	testFloatConversions(t, 1000, 9, 2)
	testFloatConversions(t, 1000, 10, 3)
	testFloatConversions(t, 1000, 11, 3)
}

func TestNegativeGaugeConversions(t *testing.T) {
	testNegativeIntConversions(t, 1000, 1, 0)
	testNegativeIntConversions(t, 1000, 3, 0)
	testNegativeIntConversions(t, 1000, 1, 2)
	testNegativeIntConversions(t, 1000, 3, 2)
}

func TestConvertFromIntFloat(t *testing.T) {
	validateConvertFromIntFloat(t, 1.0, 0, 1.0)
	validateConvertFromIntFloat(t, 2.0, 0, 2.0)
	validateConvertFromIntFloat(t, 10.0, 1, 1.0)
	validateConvertFromIntFloat(t, 200.0, 2, 2.0)
}

func TestInfNan(t *testing.T) {
	validateConvertToIntFloat(t, math.Inf(0), 0, math.Inf(0), 0, true)
	validateConvertToIntFloat(t, math.Inf(0), 3, math.Inf(0), 0, true)
	validateConvertToIntFloat(t, math.Inf(0), 6, math.Inf(0), 0, true)
	validateConvertToIntFloat(t, math.Inf(-1), 0, math.Inf(-1), 0, true)
	validateConvertToIntFloat(t, math.Inf(-1), 3, math.Inf(-1), 0, true)
	validateConvertToIntFloat(t, math.Inf(-1), 6, math.Inf(-1), 0, true)
	validateConvertToIntFloat(t, math.NaN(), 0, math.NaN(), 0, true)
	validateConvertToIntFloat(t, math.NaN(), 3, math.NaN(), 0, true)
	validateConvertToIntFloat(t, math.NaN(), 6, math.NaN(), 0, true)
}

func TestInvalidMult(t *testing.T) {
	_, _, _, err := convertToIntFloat(1.0, maxMult+1)
	require.Error(t, err)
}

func testIntConversions(t *testing.T, numConv, numDig, numDec int) {
	validateIntConversions(t, numConv, numDig, numDec, false)
}

func testNegativeIntConversions(t *testing.T, numConv, numDig, numDec int) {
	validateIntConversions(t, numConv, numDig, numDec, true)
}

func validateIntConversions(t *testing.T, numConv, numDig, numDec int, neg bool) {
	rand.Seed(time.Now().UnixNano())
	digMod := int(math.Pow10(numDig))
	decMod := int(math.Pow10(numDec))
	sign := 1.0
	if neg {
		sign = -1.0
	}
	for i := 0; i < numConv; i++ {
		var val float64
		dig := rand.Int() % digMod
		dec := rand.Int() % decMod
		if numDec == 0 {
			val = sign * float64(dig)
			validateConvertToIntFloat(t, val, numDec, val, 0, false)
		} else {
			val, _ = strconv.ParseFloat(strconv.Itoa(dig)+"."+strconv.Itoa(dec), 64)
			expDecStr := strconv.Itoa(dec)
			if len(expDecStr) < numDec {
				expDecStr += strings.Repeat("0", numDec-len(expDecStr))
			}
			expected, _ := strconv.ParseInt(strconv.Itoa(dig)+expDecStr, 10, 64)
			validateConvertToIntFloat(t, sign*val, numDec, sign*float64(expected), numDec, false)
		}
	}
}

func testFloatConversions(t *testing.T, numConv, numDig, numDec int) {
	rand.Seed(time.Now().UnixNano())
	digMod := int(math.Pow10(numDig))
	decMod := int(math.Pow10(numDec))

	for i := 0; i < numConv; i++ {
		dig := rand.Int() % digMod
		dec := rand.Int() % decMod

		expDecStr := strconv.Itoa(dec)
		if len(expDecStr) < numDec {
			expDecStr += strings.Repeat("0", numDec-len(expDecStr))
		}

		val, _ := strconv.ParseFloat(strconv.Itoa(dig)+"."+expDecStr, 64)
		validateConvertFloat(t, val, 0)
	}
}

func validateConvertToIntFloat(t *testing.T, val float64, curDec int, expectedVal float64, maxExpectedDec int, expectedFloat bool) {
	iv, dec, isFloat, err := convertToIntFloat(val, uint8(curDec))
	require.NoError(t, err)
	if math.IsNaN(val) {
		require.True(t, math.IsNaN(iv))
	} else {
		require.Equal(t, expectedVal, iv)
	}

	require.True(t, uint8(maxExpectedDec) >= dec)
	require.Equal(t, expectedFloat, isFloat)
}

func validateConvertFloat(t *testing.T, val float64, curDec int) {
	v, dec, isFloat, err := convertToIntFloat(val, uint8(curDec))
	require.NoError(t, err)
	if isFloat {
		require.Equal(t, val, v)
		require.Equal(t, uint8(0), dec)
		require.Equal(t, true, isFloat)
		return
	}

	// In the case where the randomly generated float can be converted to an int due to lack
	// of decimal places, confirm that the returned val is as expected with an error factor
	// that is less than the dec returned
	require.True(t, math.Abs(val-v/math.Pow10(int(dec))) < 1.0/math.Pow10(int(dec)))
}

func validateConvertFromIntFloat(t *testing.T, val float64, mult int, expected float64) {
	v := convertFromIntFloat(val, uint8(mult))
	require.Equal(t, expected, v)
}
