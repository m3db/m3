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

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestTimerPool(t *testing.T) {
	p := NewTimerPool(pool.NewObjectPoolOptions().SetSize(1))
	streamOpts := cm.NewOptions()
	p.Init(func() *Timer {
		return NewTimer(streamOpts)
	})

	// Retrieve a timer from the pool
	timer := p.Get()
	timer.Reset()
	timer.Add(10)
	require.Equal(t, int64(1), timer.Count())
	require.Equal(t, 10.0, timer.Sum())

	// Put the timer back to pool
	p.Put(timer)

	// Retrieve the timer
	timer = p.Get()
	timer.Reset()
	timer.Add(10)
	require.Equal(t, int64(1), timer.Count())
	require.Equal(t, 10.0, timer.Sum())
}
