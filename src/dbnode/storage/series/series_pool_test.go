// Copyright (c) 2024 Uber Technologies, Inc.
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

package series

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/pool"
)

func TestDatabaseSeriesPool(t *testing.T) {
	t.Run("NewDatabaseSeriesPool", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseSeriesPool(opts)
		require.NotNil(t, p)
	})

	t.Run("GetPut", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseSeriesPool(opts)

		// Get a series from the pool
		series := p.Get()
		require.NotNil(t, series)

		// Put the series back
		p.Put(series)

		// Get another series, should be the same one
		series2 := p.Get()
		require.NotNil(t, series2)
		require.Equal(t, series, series2)
	})

	t.Run("MultipleGetPut", func(t *testing.T) {
		size := 2
		opts := pool.NewObjectPoolOptions().SetSize(size)
		p := NewDatabaseSeriesPool(opts)

		// Get multiple series
		series1 := p.Get()
		series2 := p.Get()
		require.NotNil(t, series1)
		require.NotNil(t, series2)

		// Put them back
		p.Put(series1)
		p.Put(series2)

		// Get them again
		series1Again := p.Get()
		series2Again := p.Get()
		require.NotNil(t, series1Again)
		require.NotNil(t, series2Again)
	})

	t.Run("Reuse", func(t *testing.T) {
		opts := pool.NewObjectPoolOptions().SetSize(1)
		p := NewDatabaseSeriesPool(opts)

		// Get and put multiple times
		var lastSeries DatabaseSeries
		for i := 0; i < 10; i++ {
			series := p.Get()
			require.NotNil(t, series)
			if lastSeries != nil {
				require.Equal(t, lastSeries, series)
			}
			lastSeries = series
			p.Put(series)
		}
	})

	t.Run("Capacity", func(t *testing.T) {
		size := 2
		opts := pool.NewObjectPoolOptions().SetSize(size)
		p := NewDatabaseSeriesPool(opts)

		// Get series up to capacity
		series := make([]DatabaseSeries, size)
		for i := 0; i < size; i++ {
			series[i] = p.Get()
			require.NotNil(t, series[i])
		}

		// Put all series back
		for i := 0; i < size; i++ {
			p.Put(series[i])
		}

		// Get series again
		for i := 0; i < size; i++ {
			s := p.Get()
			require.NotNil(t, s)
		}
	})
}
