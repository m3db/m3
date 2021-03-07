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
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectPoolRefillOnLowWaterMark(t *testing.T) {
	opts := NewObjectPoolOptions().
		SetSize(100).
		SetRefillLowWatermark(0.25).
		SetRefillHighWatermark(0.75)

	pool := NewObjectPool(opts).(*objectPool)
	pool.Init(func() interface{} {
		return 1
	})

	assert.Equal(t, 100, len(pool.values))

	for i := 0; i < 74; i++ {
		pool.Get()
	}

	assert.Equal(t, 26, len(pool.values))

	// This should trigger a refill
	pool.Get()

	start := time.Now()
	for time.Since(start) < 10*time.Second {
		if len(pool.values) == 75 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Assert refilled
	assert.Equal(t, 75, len(pool.values))
}

func TestObjectPoolInitTwiceError(t *testing.T) {
	var accessErr error
	opts := NewObjectPoolOptions().SetOnPoolAccessErrorFn(func(err error) {
		accessErr = err
	})

	pool := NewObjectPool(opts)
	pool.Init(func() interface{} {
		return 1
	})

	require.NoError(t, accessErr)

	pool.Init(func() interface{} {
		return 1
	})

	assert.Error(t, accessErr)
	assert.Equal(t, errPoolAlreadyInitialized, accessErr)
}

func TestObjectPoolGetBeforeInitError(t *testing.T) {
	var accessErr error
	opts := NewObjectPoolOptions().SetOnPoolAccessErrorFn(func(err error) {
		accessErr = err
		panic(err)
	})

	pool := NewObjectPool(opts)

	require.NoError(t, accessErr)

	assert.Panics(t, func() {
		pool.Get()
	})

	assert.Error(t, accessErr)
	assert.Equal(t, errPoolAccessBeforeInitialized, accessErr)
}

func TestObjectPoolPutBeforeInitError(t *testing.T) {
	var accessErr error
	opts := NewObjectPoolOptions().SetOnPoolAccessErrorFn(func(err error) {
		accessErr = err
	})

	pool := NewObjectPool(opts)

	require.NoError(t, accessErr)

	pool.Put(1)

	assert.Error(t, accessErr)
	assert.Equal(t, errPoolAccessBeforeInitialized, accessErr)
}

func BenchmarkObjectPoolGetPut(b *testing.B) {
	opts := NewObjectPoolOptions().SetSize(1)
	pool := NewObjectPool(opts)
	pool.Init(func() interface{} {
		b := make([]byte, 0, 16)
		return &b
	})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		o := pool.Get().(*[]byte)
		_ = *o
		pool.Put(o)
	}
}

// go test -benchmem -run=^$ github.com/m3db/m3/src/x/pool -bench '^(BenchmarkObjectPoolParallel)$' -cpu 1,2,4,6,8,12
func BenchmarkObjectPoolParallelGetPut(b *testing.B) {
	type poolObj struct {
		a, b int
		c    *int64
		ts   int64
	}

	p := NewObjectPool(NewObjectPoolOptions().SetSize(1024))
	p.Init(func() interface{} {
		return &poolObj{}
	})

	ts := time.Now().UnixNano()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := p.Get()
			obj, ok := op.(*poolObj)
			if !ok {
				b.Fail()
			}
			// do something with object, so there's something going on between gets/puts:
			obj.a = b.N
			obj.c = &ts
			obj.ts = ts + int64(b.N)
			p.Put(obj)
		}
	})
}

func BenchmarkObjectPoolParallelGetMultiPutContended(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetSize(256)

	p := NewObjectPool(opts)
	p.Init(func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bufs := make([]*[]byte, 16)
			for i := 0; i < len(bufs); i++ {
				o, ok := p.Get().(*[]byte)
				if !ok {
					b.Fail()
				}
				bufs[i] = o
			}
			for i := 0; i < len(bufs); i++ {
				o := bufs[i]
				buf := *o
				buf = strconv.AppendInt(buf[:0], 12344321, 10)
				runtime.KeepAlive(buf)
				p.Put(o)
			}
		}
	})
}

//nolint:dupl
func BenchmarkObjectPoolParallelGetMultiPutContendedDynamic(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetDynamic(true)

	p := NewObjectPool(opts)
	p.Init(func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bufs := make([]*[]byte, 16)
			for i := 0; i < len(bufs); i++ {
				o, ok := p.Get().(*[]byte)
				if !ok {
					b.Fail()
				}
				bufs[i] = o
			}
			for i := 0; i < len(bufs); i++ {
				o := bufs[i]
				buf := *o
				buf = strconv.AppendInt(buf[:0], 12344321, 10)
				runtime.KeepAlive(buf)
				p.Put(o)
			}
		}
	})
}

func BenchmarkObjectPoolParallelGetMultiPutContendedWithRefill(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetSize(32).
		SetRefillLowWatermark(0.05).
		SetRefillHighWatermark(0.25)

	p := NewObjectPool(opts)
	p.Init(func() interface{} {
		b := make([]byte, 0, 32)
		return &b
	})
	objs := make([]interface{}, 16)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < len(objs); i++ {
				o := p.Get()
				objs[i] = o
			}

			for _, obj := range objs {
				o, ok := obj.(*[]byte)
				if !ok {
					b.Fail()
				}
				buf := *o
				buf = strconv.AppendInt(buf[:0], 12344321, 10)
				p.Put(o)
			}
		}
	})
}
