// Copyright (c) 2021  Uber Technologies, Inc.
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

package ptr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	assert.Equal(t, "string", ToString(String("string")))
	assert.Equal(t, "", ToString(nil))
}

func TestStringEqual(t *testing.T) {
	assert.True(t, StringEqual(nil, nil))
	assert.True(t, StringEqual(String("foo"), String("foo")))
	assert.False(t, StringEqual(nil, String("foo")))
	assert.False(t, StringEqual(String("foo"), nil))
	assert.False(t, StringEqual(String("foo"), String("bar")))
}

func TestIntEqual(t *testing.T) {
	assert.True(t, IntEqual(nil, nil))
	assert.True(t, IntEqual(Int(100), Int(100)))
	assert.False(t, IntEqual(nil, Int(100)))
	assert.False(t, IntEqual(Int(100), nil))
	assert.False(t, IntEqual(Int(100), Int(200)))
}

func TestInt32Equal(t *testing.T) {
	assert.True(t, Int32Equal(nil, nil))
	assert.True(t, Int32Equal(Int32(100), Int32(100)))
	assert.False(t, Int32Equal(nil, Int32(100)))
	assert.False(t, Int32Equal(Int32(100), nil))
	assert.False(t, Int32Equal(Int32(100), Int32(200)))
}

func TestInt64Equal(t *testing.T) {
	assert.True(t, Int64Equal(nil, nil))
	assert.True(t, Int64Equal(Int64(100), Int64(100)))
	assert.False(t, Int64Equal(nil, Int64(100)))
	assert.False(t, Int64Equal(Int64(100), nil))
	assert.False(t, Int64Equal(Int64(100), Int64(200)))
}

func TestBoolEqual(t *testing.T) {
	assert.True(t, BoolEqual(nil, nil))
	assert.True(t, BoolEqual(Bool(false), Bool(false)))
	assert.True(t, BoolEqual(Bool(true), Bool(true)))
	assert.False(t, BoolEqual(nil, Bool(true)))
	assert.False(t, BoolEqual(Bool(true), nil))
	assert.False(t, BoolEqual(Bool(true), Bool(false)))
}

func TestFloat64Equal(t *testing.T) {
	assert.True(t, Float64Equal(nil, nil))
	assert.True(t, Float64Equal(Float64(100.56), Float64(100.56)))
	assert.False(t, Float64Equal(nil, Float64(100.56)))
	assert.False(t, Float64Equal(Float64(100.56), nil))
	assert.False(t, Float64Equal(Float64(100.56), Float64(200.56)))
}

func TestUint(t *testing.T) {
	assert.Equal(t, uint(5), ToUInt(UInt(5)))
	assert.Equal(t, uint(0), ToUInt(UInt(0)))
	assert.Equal(t, uint(0), ToUInt(nil))
}
