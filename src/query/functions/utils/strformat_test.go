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

package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueToProm(t *testing.T) {
	assert.Equal(t, FormatFloat(1.0), "1")
	assert.Equal(t, FormatFloat(1.2), "1.2")
	assert.Equal(t, FormatFloat(math.NaN()), "NaN")
	assert.Equal(t, FormatFloat(math.Inf(1)), "+Inf")
	assert.Equal(t, FormatFloat(math.Inf(-1)), "-Inf")
	assert.Equal(t, FormatFloat(0.0119311), "0.0119311")
}

func TestValueToPromBytes(t *testing.T) {
	assert.Equal(t, FormatFloatToBytes(1.0), []byte("1"))
	assert.Equal(t, FormatFloatToBytes(1.2), []byte("1.2"))
	assert.Equal(t, FormatFloatToBytes(math.NaN()), []byte("NaN"))
	assert.Equal(t, FormatFloatToBytes(math.Inf(1)), []byte("+Inf"))
	assert.Equal(t, FormatFloatToBytes(math.Inf(-1)), []byte("-Inf"))
	assert.Equal(t, FormatFloatToBytes(0.0119311), []byte("0.0119311"))
}
