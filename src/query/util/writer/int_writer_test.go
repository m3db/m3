// Copyright (c) 2019 Uber Technologies, Inc.
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

package writer

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntegerLength(t *testing.T) {
	for i := 0; i < 18; i++ {
		small := int(math.Pow(10, float64(i)))
		large := small*10 - 1
		expected := i + 1
		assert.Equal(t, expected, IntLength(small))
		assert.Equal(t, expected, IntLength(large))
	}

	assert.Equal(t, 19, IntLength(1000000000000000000))
}

func TestWriteInteger(t *testing.T) {
	ints := make([]int, 105)
	for i := range ints {
		l := IntLength(i)
		buf := make([]byte, l)
		idx := WriteInteger(buf, i, 0)
		assert.Equal(t, l, idx)
		assert.Equal(t, []byte(fmt.Sprint(i)), buf)
	}
}

func TestWriteIntegersAtIndex(t *testing.T) {
	l := IntLength(345) + IntLength(12)
	buf := make([]byte, l)
	idx := WriteInteger(buf, 12, 0)
	idx = WriteInteger(buf, 345, idx)
	assert.Equal(t, 5, idx)
	assert.Equal(t, []byte("12345"), buf)
}
