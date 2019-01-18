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

	"github.com/stretchr/testify/require"

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

func TestWriteIntegersSingle(t *testing.T) {
	sep := byte('!')
	ints := []int{1}
	l := IntsLength(ints)
	assert.Equal(t, 1, l)

	buf := make([]byte, l)
	idx := WriteIntegers(buf, ints, sep, 0)
	assert.Equal(t, l, idx)
	expected := []byte("1")
	assert.Equal(t, expected, buf)

	ints = []int{10}
	l = IntsLength(ints)
	assert.Equal(t, 2, l)
	buf = make([]byte, l)
	idx = WriteIntegers(buf, ints, sep, 0)
	assert.Equal(t, l, idx)
	expected = []byte("10")
	assert.Equal(t, expected, buf)
}

func TestWriteIntegersSingleAtIndex(t *testing.T) {
	sep := byte('!')
	ints := []int{1}
	buf := make([]byte, 2)
	buf[0] = byte('?')
	idx := WriteIntegers(buf, ints, sep, 1)
	assert.Equal(t, 3, idx)
	expected := []byte("?1")
	assert.Equal(t, expected, buf)

	idx = 0
	idx = WriteIntegers(buf, ints, sep, idx)
	idx = WriteIntegers(buf, ints, sep, idx)
	assert.Equal(t, 3, idx)
	expected = []byte("11")
	assert.Equal(t, expected, buf)
}

func TestWriteIntegersMultiple(t *testing.T) {
	sep := byte('!')
	ints := []int{1, 2}
	l := IntsLength(ints)
	assert.Equal(t, 3, l)

	buf := make([]byte, l)
	idx := WriteIntegers(buf, ints, sep, 0)
	assert.Equal(t, l, idx)
	expected := []byte("1!2")
	require.Equal(t, expected, buf)

	ints = []int{10, 20}
	l = IntsLength(ints)
	assert.Equal(t, 5, l)
	buf = make([]byte, l)
	idx = WriteIntegers(buf, ints, sep, 0)
	assert.Equal(t, l, idx)
	expected = []byte("10!20")
	assert.Equal(t, expected, buf)
}

func TestWriteIntegersMultipleAtIndex(t *testing.T) {
	sep := byte('!')
	ints := []int{1, 20, 300, 4000, 50000}
	l := IntsLength(ints)
	assert.Equal(t, 19, l)

	buf := make([]byte, l+2)
	buf[0] = '?'
	buf[l+1] = '?'
	idx := WriteIntegers(buf, ints, sep, 1)
	assert.Equal(t, l+1, idx)
	expected := []byte("?1!20!300!4000!50000?")
	require.Equal(t, expected, buf)
}
