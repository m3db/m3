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

package leakcheckpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type getFn func() elemType
type putFn func(elemType)

type testElemTypePool struct {
	getFn getFn
	putFn putFn
}

func (e *testElemTypePool) Init()          { panic("not implemented") }
func (e *testElemTypePool) Get() elemType  { return e.getFn() }
func (e *testElemTypePool) Put(v elemType) { e.putFn(v) }

func TestInit(t *testing.T) {
	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{})
	assert.Panics(t, func() {
		debugPool.Init()
	})
}

func TestGetPut(t *testing.T) {
	var (
		empty elemType
	)

	getFn := func() elemType { return empty }
	putFn := func(elemType) {}

	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{getFn: getFn, putFn: putFn})
	val := debugPool.Get()
	require.Equal(t, empty, val)

	debugPool.Lock()
	require.Equal(t, 1, debugPool.NumGets)
	require.Equal(t, 1, len(debugPool.AllGetItems))
	require.Equal(t, empty, debugPool.AllGetItems[0].Value)
	require.Equal(t, 1, len(debugPool.PendingItems))
	require.Equal(t, empty, debugPool.PendingItems[0].Value)
	debugPool.Unlock()

	debugPool.Put(empty)
	debugPool.Lock()
	require.Equal(t, 1, debugPool.NumGets)
	require.Equal(t, 1, debugPool.NumPuts)
	require.Equal(t, 1, len(debugPool.AllGetItems))
	require.Equal(t, empty, debugPool.AllGetItems[0].Value)
	require.Equal(t, 0, len(debugPool.PendingItems))
	debugPool.Unlock()
}

func TestDisallowUnalloc(t *testing.T) {
	putFn := func(v elemType) {
		panic("should never get here")
	}
	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{
		DisallowUntrackedPuts: true,
	}, &testElemTypePool{putFn: putFn})

	var (
		empty elemType
	)
	require.Panics(t, func() {
		debugPool.Put(empty)
	})
}

func TestAllowUnalloc(t *testing.T) {
	var (
		empty elemType
		call  int
	)
	putFn := func(v elemType) {
		call++
		require.Equal(t, empty, v)
	}
	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{putFn: putFn})

	require.NotPanics(t, func() {
		debugPool.Put(empty)
	})
	require.Equal(t, 1, call)

	debugPool.Lock()
	require.Equal(t, 0, debugPool.NumGets)
	require.Equal(t, 1, debugPool.NumPuts)
	require.Equal(t, 0, len(debugPool.AllGetItems))
	require.Equal(t, 0, len(debugPool.PendingItems))
	debugPool.Unlock()
}

func TestStacksDiffer(t *testing.T) {
	var (
		empty elemType
	)

	getFn := func() elemType {
		return empty
	}

	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{getFn: getFn})
	v1 := debugPool.Get()
	v2 := debugPool.Get()

	debugPool.Lock()
	require.Equal(t, 2, debugPool.NumGets)
	require.Equal(t, 2, len(debugPool.AllGetItems))
	require.Equal(t, v1, debugPool.AllGetItems[0].Value)
	require.Equal(t, v2, debugPool.AllGetItems[1].Value)
	require.Equal(t, 2, len(debugPool.PendingItems))
	require.Equal(t, v1, debugPool.PendingItems[0].Value)
	require.Equal(t, v2, debugPool.PendingItems[1].Value)
	require.NotEqual(t, debugPool.AllGetItems[0].GetStacktrace, debugPool.AllGetItems[1].GetStacktrace)
	debugPool.Unlock()
}

func TestCheck(t *testing.T) {
	var (
		empty elemType
	)

	getFn := func() elemType { return empty }
	putFn := func(elemType) {}

	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{getFn: getFn, putFn: putFn})
	defer debugPool.Check(t)
	val := debugPool.Get()
	require.Equal(t, empty, val)
	debugPool.Put(val)
}

func TestCheckExtended(t *testing.T) {
	var (
		empty elemType
	)

	getFn := func() elemType { return empty }
	putFn := func(elemType) {}

	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{}, &testElemTypePool{getFn: getFn, putFn: putFn})
	defer debugPool.CheckExtended(t, func(e leakcheckElemType) {
		require.Equal(t, empty, e.Value, string(e.GetStacktrace))
	})
	val := debugPool.Get()
	debugPool.Put(val)
}

func TestGetHookFn(t *testing.T) {
	var (
		x = elemType(1)
	)

	getFn := func() elemType { return x }
	putFn := func(elemType) {}

	debugPool := newLeakcheckElemTypePool(leakcheckElemTypePoolOpts{
		GetHookFn: func(e elemType) elemType { return nil },
	}, &testElemTypePool{getFn: getFn, putFn: putFn})
	defer debugPool.Check(t)
	val := debugPool.Get()
	require.Nil(t, val)
	require.NotNil(t, x)
	debugPool.Put(val)
}
