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

package writer

import (
	"errors"
	"math/rand"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

var (
	errWriterClosed = errors.New("writer is closed")
)

type shardedWriterMetrics struct {
	writerClosed  tally.Counter
	encodeSuccess tally.Counter
	encodeErrors  tally.Counter
	routeSuccess  tally.Counter
	routeErrors   tally.Counter
}

func newShardedWriterMetrics(scope tally.Scope) shardedWriterMetrics {
	encodeScope := scope.SubScope("encode")
	routeScope := scope.SubScope("route")
	return shardedWriterMetrics{
		writerClosed:  scope.Counter("writer-closed"),
		encodeSuccess: encodeScope.Counter("success"),
		encodeErrors:  encodeScope.Counter("errors"),
		routeSuccess:  routeScope.Counter("success"),
		routeErrors:   routeScope.Counter("errors"),
	}
}

type shardFn func(chunkedID id.ChunkedID) uint32
type randFn func() float64

// shardedWriter encodes data in a shard-aware fashion and routes them to the backend.
// shardedWriter is not thread safe.
type shardedWriter struct {
	sharding.AggregatedSharder
	common.Router

	nowFn                    clock.NowFn
	maxBufferSize            int
	bufferedEncoderPool      msgpack.BufferedEncoderPool
	encodingTimeSamplingRate float64

	closed          bool
	rand            *rand.Rand
	encodersByShard []msgpack.AggregatedEncoder
	metrics         shardedWriterMetrics
	shardFn         shardFn
	randFn          randFn
}

// NewShardedWriter creates a new sharded writer.
func NewShardedWriter(
	sharderID sharding.SharderID,
	router common.Router,
	opts Options,
) (Writer, error) {
	sharder, err := sharding.NewAggregatedSharder(sharderID)
	if err != nil {
		return nil, err
	}
	numShards := sharderID.NumShards()
	nowFn := opts.ClockOptions().NowFn()
	instrumentOpts := opts.InstrumentOptions()
	w := &shardedWriter{
		AggregatedSharder:        sharder,
		Router:                   router,
		nowFn:                    nowFn,
		maxBufferSize:            opts.MaxBufferSize(),
		bufferedEncoderPool:      opts.BufferedEncoderPool(),
		encodingTimeSamplingRate: opts.EncodingTimeSamplingRate(),
		rand:            rand.New(rand.NewSource(nowFn().UnixNano())),
		encodersByShard: make([]msgpack.AggregatedEncoder, numShards),
		metrics:         newShardedWriterMetrics(instrumentOpts.MetricsScope()),
	}
	w.shardFn = w.Shard
	w.randFn = w.rand.Float64
	return w, nil
}

func (w *shardedWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	shard := w.shardFn(mp.ChunkedID)
	encoder := w.encodersByShard[shard]
	if encoder == nil {
		bufferedEncoder := w.bufferedEncoderPool.Get()
		bufferedEncoder.Reset()
		encoder = msgpack.NewAggregatedEncoder(bufferedEncoder)
		w.encodersByShard[shard] = encoder
	}
	return w.encode(encoder, mp, shard)
}

func (w *shardedWriter) Flush() error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	multiErr := xerrors.NewMultiError()
	for shard, encoder := range w.encodersByShard {
		if encoder == nil {
			continue
		}
		bufferedEncoder := encoder.Encoder()
		buffer := bufferedEncoder.Buffer()
		if buffer.Len() == 0 {
			continue
		}
		newBufferedEncoder := w.bufferedEncoderPool.Get()
		newBufferedEncoder.Reset()
		encoder.Reset(newBufferedEncoder)
		if err := w.Route(uint32(shard), common.NewRefCountedBuffer(bufferedEncoder)); err != nil {
			w.metrics.routeErrors.Inc(1)
			multiErr = multiErr.Add(err)
		} else {
			w.metrics.routeSuccess.Inc(1)
		}
	}
	return multiErr.FinalError()
}

func (w *shardedWriter) Close() error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	err := w.Flush()
	w.closed = true
	return err
}

func (w *shardedWriter) encode(
	encoder msgpack.AggregatedEncoder,
	mp aggregated.ChunkedMetricWithStoragePolicy,
	shard uint32,
) error {
	bufferedEncoder := encoder.Encoder()
	buffer := bufferedEncoder.Buffer()
	sizeBefore := buffer.Len()

	// Encode data.
	var err error
	if w.encodingTimeSamplingRate > 0 && w.randFn() < w.encodingTimeSamplingRate {
		encodedAtNanos := w.nowFn().UnixNano()
		err = encoder.EncodeChunkedMetricWithStoragePolicyAndEncodeTime(mp, encodedAtNanos)
	} else {
		err = encoder.EncodeChunkedMetricWithStoragePolicy(mp)
	}
	if err != nil {
		w.metrics.encodeErrors.Inc(1)
		buffer.Truncate(sizeBefore)
		// Clear out the encoder error.
		encoder.Reset(bufferedEncoder)
		return err
	}
	w.metrics.encodeSuccess.Inc(1)

	// If the buffer size is not big enough, do nothing.
	sizeAfter := buffer.Len()
	if sizeAfter < w.maxBufferSize {
		return nil
	}
	// Otherwise we get a new buffer and copy the bytes exceeding the max
	// flush size to it, swap the new buffer with the old one, and flush out
	// the old buffer.
	bufferedEncoder2 := w.bufferedEncoderPool.Get()
	bufferedEncoder2.Reset()
	if sizeBefore > 0 {
		data := bufferedEncoder.Bytes()
		bufferedEncoder2.Buffer().Write(data[sizeBefore:sizeAfter])
		buffer.Truncate(sizeBefore)
	}
	encoder.Reset(bufferedEncoder2)
	if err := w.Route(shard, common.NewRefCountedBuffer(bufferedEncoder)); err != nil {
		w.metrics.routeErrors.Inc(1)
		return err
	}
	w.metrics.routeSuccess.Inc(1)
	return nil
}
