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

package writer

import (
	"errors"
	"math/rand"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3x/clock"
	murmur3 "github.com/m3db/stackmurmur3"

	"github.com/uber-go/tally"
)

var (
	errWriterClosed = errors.New("writer is closed")
)

type protobufWriterMetrics struct {
	writerClosed  tally.Counter
	encodeSuccess tally.Counter
	encodeErrors  tally.Counter
	routeSuccess  tally.Counter
	routeErrors   tally.Counter
}

func newProtobufWriterMetrics(scope tally.Scope) protobufWriterMetrics {
	encodeScope := scope.SubScope("encode")
	routeScope := scope.SubScope("route")
	return protobufWriterMetrics{
		writerClosed:  scope.Counter("writer-closed"),
		encodeSuccess: encodeScope.Counter("success"),
		encodeErrors:  encodeScope.Counter("errors"),
		routeSuccess:  routeScope.Counter("success"),
		routeErrors:   routeScope.Counter("errors"),
	}
}

type idShardFn func([]byte) uint32

// protobufWriter encodes data and routes them to the backend.
// protobufWriter is not thread safe.
type protobufWriter struct {
	encodingTimeSamplingRate float64
	encoder                  protobuf.AggregatedEncoder
	p                        producer.Producer
	numShards                uint32

	closed  bool
	m       aggregated.MetricWithStoragePolicy
	rand    *rand.Rand
	metrics protobufWriterMetrics

	nowFn   clock.NowFn
	randFn  randFn
	shardFn idShardFn
}

// NewProtobufWriter creates a writer that encodes metric in protobuf.
func NewProtobufWriter(
	producer producer.Producer,
	opts Options,
) Writer {
	nowFn := opts.ClockOptions().NowFn()
	instrumentOpts := opts.InstrumentOptions()
	w := &protobufWriter{
		encodingTimeSamplingRate: opts.EncodingTimeSamplingRate(),
		encoder:                  protobuf.NewAggregatedEncoder(opts.BytesPool()),
		p:                        producer,
		numShards:                producer.NumShards(),
		closed:                   false,
		rand:                     rand.New(rand.NewSource(nowFn().UnixNano())),
		metrics:                  newProtobufWriterMetrics(instrumentOpts.MetricsScope()),
		nowFn:                    nowFn,
	}
	w.randFn = w.rand.Float64
	w.shardFn = w.shard
	return w
}

func (w *protobufWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	var encodeNanos int64
	if w.encodingTimeSamplingRate > 0 && w.randFn() < w.encodingTimeSamplingRate {
		encodeNanos = w.nowFn().UnixNano()
	}
	m, shard := w.prepare(mp)
	if err := w.encoder.Encode(m, encodeNanos); err != nil {
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	w.metrics.encodeSuccess.Inc(1)
	if err := w.p.Produce(newMessage(shard, w.encoder.Buffer())); err != nil {
		w.metrics.routeErrors.Inc(1)
		return err
	}
	w.metrics.routeSuccess.Inc(1)
	return nil
}

func (w *protobufWriter) shard(b []byte) uint32 {
	return murmur3.Sum32(b) % w.numShards
}

func (w *protobufWriter) prepare(mp aggregated.ChunkedMetricWithStoragePolicy) (aggregated.MetricWithStoragePolicy, uint32) {
	// TODO(cw) Chunked metric has no 'type' field, consider adding one.
	w.m.ID = w.m.ID[:0]
	w.m.ID = append(w.m.ID, mp.Prefix...)
	w.m.ID = append(w.m.ID, mp.Data...)
	w.m.ID = append(w.m.ID, mp.Suffix...)
	w.m.Metric.TimeNanos = mp.TimeNanos
	w.m.Metric.Value = mp.Value
	w.m.StoragePolicy = mp.StoragePolicy
	shard := w.shardFn(w.m.ID)
	return w.m, shard
}

func (w *protobufWriter) Flush() error {
	return nil
}

func (w *protobufWriter) Close() error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	// Don't close the producer here, it maybe shared by other writers.
	w.closed = true
	return nil
}

type message struct {
	shard uint32
	data  protobuf.Buffer
}

func newMessage(shard uint32, data protobuf.Buffer) producer.Message {
	return message{shard: shard, data: data}
}

func (d message) Shard() uint32 {
	return d.shard
}

func (d message) Bytes() []byte {
	return d.data.Bytes()
}

func (d message) Size() int {
	// Use the cap of the underlying byte slice in the buffer instead of
	// the length of the byte encoded to avoid "memory leak", for example
	// when the underlying buffer is 2KB, and it only encoded 300B, if we
	// use 300 as the size, then a producer with a buffer of 3GB could be
	// actually buffering 20GB in total for the underlying buffers.
	return cap(d.data.Bytes())
}

func (d message) Finalize(producer.FinalizeReason) {
	d.data.Close()
}
