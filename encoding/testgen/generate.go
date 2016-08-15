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

package testgen

import (
	"math"
	"math/rand"
	"strconv"
)

// GenerateFloatVal generates a random float val given the number of digits and decimal places
func GenerateFloatVal(r *rand.Rand, numDig, numDec int) float64 {
	var val float64
	digMod := int(math.Pow10(numDig))
	decMod := int(math.Pow10(numDec))
	dig := r.Int() % digMod

	if numDec == 0 {
		val = float64(dig)
	} else {
		dec := r.Int() % decMod
		val, _ = strconv.ParseFloat(strconv.Itoa(dig)+"."+strconv.Itoa(dec), 64)
	}

	return val
}
