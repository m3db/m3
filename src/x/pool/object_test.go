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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getPoolObjectSize(p *objectPool) int {
	var count int
	for i := range p.shards {
		count += len(p.shards[i].values)
	}
	return count
}

func TestObjectPoolRefillOnLowWaterMark(t *testing.T) {
	opts := NewObjectPoolOptions().
		SetShardCount(1).
		SetSize(100).
		SetRefillLowWatermark(0.25).
		SetRefillHighWatermark(0.75)

	pool := NewObjectPool(opts).(*objectPool)
	pool.Init(func() interface{} {
		return 1
	})
	defer pool.Close()

	for i := 0; i < 75; i++ {
		v := pool.Get()
		i, ok := v.(int)
		if !ok || i != 1 {
			t.Fail()
		}
	}

	// Assert refilled
	size := getPoolObjectSize(pool)
	if size < 25 {
		t.Fatalf("pool was not refiled, size %v is less than high watermark", size)
	}
}

func TestObjectPoolRefillOnLowWaterMarkSharded(t *testing.T) {
	opts := NewObjectPoolOptions().
		SetShardCount(4).
		SetSize(2000).
		SetRefillLowWatermark(0.25).
		SetRefillHighWatermark(0.75)

	pool := NewObjectPool(opts).(*objectPool)
	pool.Init(func() interface{} {
		return 1
	})
	defer pool.Close()

	for i := 0; i < 2500; i++ {
		v := pool.Get()
		i, ok := v.(int)
		if !ok || i != 1 {
			t.Fail()
		}
	}

	// Assert refilled
	size := getPoolObjectSize(pool)
	if size < 500 {
		t.Fatalf("pool was not refiled, size %v is less than 500", size)
	}
}

func TestObjectPoolDoubleInit(t *testing.T) {
	pool := NewObjectPool(NewObjectPoolOptions().SetSize(1))
	pool.Init(func() interface{} { return nil })
	assert.Panics(t, func() {
		pool.Init(func() interface{} { return nil })
	})
}

func BenchmarkObjectPoolGetPut(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetSize(1).
		SetShardCount(1)
	pool := NewObjectPool(opts)
	pool.Init(func() interface{} {
		return make([]byte, 0, 16)
	})

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		o := pool.Get()
		pool.Put(o)
	}
}

func BenchmarkObjectPoolGetMultiPutContended(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetShardCount(8).
		SetSize(256)
	p := NewObjectPool(opts)
	p.Init(func() interface{} {
		return make([]byte, 0, 32)
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
				o, ok := obj.([]byte)
				if !ok {
					b.Fail()
				}
				o = strconv.AppendInt(o[:0], 12344321, 10)
				p.Put(o)
			}
		}
	})
}

func BenchmarkObjectPoolGetMultiPutContendedWithRefill(b *testing.B) {
	opts := NewObjectPoolOptions().
		SetShardCount(8).
		SetSize(32).
		SetRefillLowWatermark(0.05).
		SetRefillHighWatermark(0.25)

	p := NewObjectPool(opts)
	p.Init(func() interface{} {
		return make([]byte, 0, 32)
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
				o, ok := obj.([]byte)
				if !ok {
					b.Fail()
				}
				o = strconv.AppendInt(o[:0], 12344321, 10)
				p.Put(o)
			}
		}
	})
}

// go test -benchmem -run=^$ github.com/m3db/m3/src/x/pool -bench '^(BenchmarkObjectPoolParallel)$' -cpu 1,2,4,6,8,12
func BenchmarkObjectPoolParallel(b *testing.B) {
	type poolObj struct {
		arr  []byte
		a, b int
		c    *bool
		ts   int64
	}

	p := NewObjectPool(NewObjectPoolOptions().SetSize(1024))
	p.Init(func() interface{} {
		return &poolObj{
			arr: make([]byte, 0, 16),
		}
	})
	defer p.Close()

	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := p.Get()
			obj, ok := op.(*poolObj)
			if !ok {
				b.Fail()
			}
			// do something with object:
			obj.a = b.N
			obj.b = len(obj.arr)
			obj.ts = now.Unix() + int64(b.N)
			p.Put(op)
		}
	})
}
