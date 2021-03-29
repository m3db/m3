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

package client

import (
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"

	"github.com/uber-go/tally"
)

const ()

var (
	testLargerBatchTimer = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345, 222.22, 345.67, 901.23345},
	}
)

func BenchmarkParallelWriter(b *testing.B) {
	numShards := 16
	opts := NewOptions()
	writer := &writer{
		log:             opts.InstrumentOptions().Logger(),
		metrics:         newWriterMetrics(tally.NoopScope),
		encoderOpts:     opts.EncoderOptions(),
		flushSize:       opts.FlushSize(),
		queue:           testNoOpQueue{},
		encodersByShard: make(map[uint32]*lockedEncoder),
	}
	writer.newLockedEncoderFn = newLockedEncoder

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    testLargerBatchTimer,
					metadatas: testStagedMetadatas,
				},
			}
			if err := writer.Write(uint32(shard), payload); err != nil {
				b.Fatalf("failed to successfully write metric: %v", err)
			}
		}
	})
}

func BenchmarkSerialOneShardWriter(b *testing.B) {
	numShards := 1
	opts := NewOptions()
	writer := &writer{
		log:             opts.InstrumentOptions().Logger(),
		metrics:         newWriterMetrics(tally.NoopScope),
		encoderOpts:     opts.EncoderOptions(),
		flushSize:       opts.FlushSize(),
		queue:           testNoOpQueue{},
		encodersByShard: make(map[uint32]*lockedEncoder),
	}
	writer.newLockedEncoderFn = newLockedEncoder

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    testLargerBatchTimer,
					metadatas: testStagedMetadatas,
				},
			}
			if err := writer.Write(uint32(shard), payload); err != nil {
				b.Fatalf("failed to successfully write metric: %v", err)
			}
		}
	})
}

func BenchmarkSerialWriter(b *testing.B) {
	numShards := 16
	opts := NewOptions()
	encoder := newLockedEncoder(opts.EncoderOptions())
	w := &writer{
		log:             opts.InstrumentOptions().Logger(),
		metrics:         newWriterMetrics(tally.NoopScope),
		encoderOpts:     opts.EncoderOptions(),
		flushSize:       opts.FlushSize(),
		queue:           testNoOpQueue{},
		encodersByShard: make(map[uint32]*lockedEncoder),
	}
	w.newLockedEncoderFn = newLockedEncoder
	writer := &testSerialWriter{
		writer:  w,
		encoder: encoder,
	}

	b.RunParallel(func(pb *testing.PB) {
		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		for pb.Next() {
			shard := r.Intn(numShards)
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    testLargerBatchTimer,
					metadatas: testStagedMetadatas,
				},
			}
			if err := writer.Write(uint32(shard), payload); err != nil {
				b.Fatalf("failed to successfully write metric: %v", err)
			}
		}
	})
}

type testNoOpQueue struct{}

func (q testNoOpQueue) Enqueue(protobuf.Buffer) error { return nil }
func (q testNoOpQueue) Close() error                  { return nil }
func (q testNoOpQueue) Size() int                     { return 0 }

type testSerialWriter struct {
	*writer

	encoder *lockedEncoder
}

func (mw *testSerialWriter) Write(
	_ uint32,
	payload payloadUnion,
) error {
	return mw.encodeWithLock(mw.encoder, payload)
}
