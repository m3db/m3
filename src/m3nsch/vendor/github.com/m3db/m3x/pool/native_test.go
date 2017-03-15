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

package pool

import (
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type I interface {
	Set(int)
	Get() int
}

type T struct {
	x I
	y [4096]byte
	z int
}

func TestNativePoolBasics(t *testing.T) {
	ts := NewNativePool(NativePoolOptions{
		Size: 1,
		Type: reflect.TypeOf(T{})})

	var (
		v  = ts.Get().(*T)
		wg sync.WaitGroup
	)

	wg.Add(1)

	go func() {
		require.True(t, ts.Owns(ts.Get()))
		wg.Done()
	}()

	ts.Put(v)
	wg.Wait()
}

func TestNativePoolOverflow(t *testing.T) {
	ts := NewNativePool(NativePoolOptions{
		Size: 1,
		Type: reflect.TypeOf(T{})})

	fn := func() interface{} {
		return &T{}
	}

	require.True(t, ts.Owns(ts.GetOr(fn)))
	require.False(t, ts.Owns(ts.GetOr(fn)))
}

type U struct {
	v int
}

func (u *U) Set(v int) {
	u.v = v
}

func (u *U) Get() int {
	return u.v
}

func TestNativePoolNesting(t *testing.T) {
	us := NewNativePool(NativePoolOptions{
		Size: 50000,
		Type: reflect.TypeOf(U{})})

	ts := NewNativePool(NativePoolOptions{
		Construct: func(ptr interface{}) {
			ptr.(*T).x = us.Get().(I)
		},
		Size: 50000,
		Type: reflect.TypeOf(T{})})

	vs := make([]*T, 100000)

	for i := range vs {
		vs[i] = ts.GetOr(func() interface{} {
			return &T{x: &U{}} // Direct allocation.
		}).(*T)

		vs[i].x.Set(i)
	}

	runtime.GC()

	for i := range vs {
		require.Equal(t, i, vs[i].x.Get())
	}
}

func TestNativePoolErrors(t *testing.T) {
	require.Panics(t, func() {
		NewNativePool(NativePoolOptions{Size: 0})
	})
}

func BenchmarkNativePool(b *testing.B) {
	ts := NewNativePool(NativePoolOptions{
		Size: 5,
		Type: reflect.TypeOf(T{})})

	for n := 0; n <= b.N; n++ {
		ts.Put(ts.Get())
	}
}
