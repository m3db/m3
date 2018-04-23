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

package arraypool

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestArrayPool(t *testing.T) {
	pool := newElemArrayPool(elemArrayPoolOpts{
		Capacity: 10,
		Options:  pool.NewObjectPoolOptions(),
	})
	require.NotNil(t, pool)

	pool.Init()
	e := pool.Get()
	require.NotNil(t, e)
	pool.Put(e)
}

func TestArrayPoolPut(t *testing.T) {
	var original []elemType
	finalizeFn := func(arr []elemType) []elemType {
		require.Equal(t, original, arr)
		return arr
	}
	pool := newElemArrayPool(elemArrayPoolOpts{
		Capacity:   10,
		Options:    pool.NewObjectPoolOptions(),
		FinalizeFn: finalizeFn,
	})
	require.NotNil(t, pool)
	pool.Init()
	original = pool.Get()
	require.NotNil(t, original)
	pool.Put(original)
}

func TestElemArrGrow(t *testing.T) {
	var results elemArr
	require.Len(t, results, 0)
	results = results.grow(10)
	require.Len(t, results, 10)
	results = results.grow(100)
	require.Len(t, results, 100)
}

func TestElemArrGrowResetsValue(t *testing.T) {
	var (
		empty   elemType
		results elemArr
	)
	require.Len(t, results, 0)
	results = results.grow(10)
	require.Len(t, results, 10)
	for _, elem := range results {
		require.Equal(t, empty, elem)
	}
}

func TestElemArrGrowPanic(t *testing.T) {
	var results elemArr
	require.Len(t, results, 0)
	results = results.grow(100)
	require.Len(t, results, 100)
	results = results[:0]
	results = results.grow(1)
	require.Len(t, results, 1)
}
