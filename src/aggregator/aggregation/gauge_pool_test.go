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

package aggregation

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestGaugePool(t *testing.T) {
	p := NewGaugePool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init()

	// Retrieve a gauge from the pool
	gauge := p.Get()
	gauge.Reset()
	gauge.Add(1.23)
	require.Equal(t, 1.23, gauge.Value())

	// Put the gauge back to pool
	p.Put(gauge)

	// Retrieve the gauge and assert it's the same gauge
	gauge = p.Get()
	require.Equal(t, 1.23, gauge.Value())

	// Reset the gauge and assert it's been reset
	gauge.Reset()
	require.Equal(t, 0.0, gauge.Value())
}
