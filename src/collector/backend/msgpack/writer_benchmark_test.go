// Copyright (c) 2017 Uber Technologies, Inc.
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

package msgpack

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
)

const (
	numShards  = 16
	numWriters = 3
)

var (
	testLargerBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345},
	}
)

func BenchmarkParallelWriter(b *testing.B) {
	runtime.GOMAXPROCS(numWriters)
	queue := &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error { return nil },
	}

	opts := NewServerOptions()
	writer := &writer{
		log:             opts.InstrumentOptions().Logger(),
		flushSize:       opts.FlushSize(),
		encoderPool:     opts.BufferedEncoderPool(),
		queue:           queue,
		encodersByShard: make(map[uint32]*lockedEncoder),
	}
	writer.newLockedEncoderFn = newLockedEncoder

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			writer.Write(uint32(shard), testLargerBatchTimer, testPoliciesList)
		}
	})
}

func BenchmarkSerialOneShardWriter(b *testing.B) {
	runtime.GOMAXPROCS(numWriters)
	numShards := 1
	queue := &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error { return nil },
	}

	opts := NewServerOptions()
	writer := &writer{
		log:             opts.InstrumentOptions().Logger(),
		flushSize:       opts.FlushSize(),
		encoderPool:     opts.BufferedEncoderPool(),
		queue:           queue,
		encodersByShard: make(map[uint32]*lockedEncoder),
	}
	writer.newLockedEncoderFn = newLockedEncoder

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			writer.Write(uint32(shard), testLargerBatchTimer, testPoliciesList)
		}
	})
}

func BenchmarkSerialWriter(b *testing.B) {
	runtime.GOMAXPROCS(numWriters)
	queue := &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error { return nil },
	}

	opts := NewServerOptions()
	pool := opts.BufferedEncoderPool()
	encoder := newLockedEncoder(pool)
	w := &writer{
		flushSize:   opts.FlushSize(),
		encoderPool: pool,
		queue:       queue,
	}
	writer := &mockSerialInstanceWriter{
		writer:  *w,
		encoder: encoder,
	}

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			writer.Write(uint32(shard), testLargerBatchTimer, testPoliciesList)
		}
	})
}

type mockSerialInstanceWriter struct {
	writer

	encoder *lockedEncoder
}

func (mw *mockSerialInstanceWriter) Flush() error { return nil }
func (mw *mockSerialInstanceWriter) Close() error { return nil }

func (mw *mockSerialInstanceWriter) Write(
	shard uint32,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	return mw.encodeWithLock(mw.encoder, mu, pl)
}
