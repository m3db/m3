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

package ts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemsetValues(t *testing.T) {
	values := NewFixedStepValues(1000, 10000, 1, time.Now())
	for i := 0; i < values.Len(); i++ {
		assert.InDelta(t, values.ValueAt(i), 1, 0.00000001)
	}
}

func TestMemsetZeroValues(t *testing.T) {
	values := NewFixedStepValues(1000, 10000, 0, time.Now())
	assert.InDelta(t, values.ValueAt(0), 0, 0.00000001)
}

func setValues(values []float64, initialValue float64) {
	for i := 0; i < len(values); i++ {
		values[i] = initialValue
	}
}
func BenchmarkMemsetZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		Memset(values, 0)
	}

}

func BenchmarkLoopZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		setValues(values, 0)
	}
}

func BenchmarkMemsetNonZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		Memset(values, 1)
	}

}

func BenchmarkLoopNonZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		setValues(values, 1)
	}
}
