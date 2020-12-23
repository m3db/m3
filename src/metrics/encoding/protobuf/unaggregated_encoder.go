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

package protobuf

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/pool"
)

const (
	// maximum number of bytes to encode message size.
	maxMessageSizeInBytes = binary.MaxVarintLen32
)

// UnaggregatedEncoder encodes unaggregated metrics.
type UnaggregatedEncoder interface {
	// Len returns the number of bytes accumulated in the encoder so far.
	Len() int

	// Reset resets the encoder buffer with optional initial data.
	Reset(initData []byte)

	// Truncate discards all but the first n encoded bytes but continues to use
	// the same allocated storage. If n is negative or greater than the length of
	// encoded buffer, an error is returned.
	Truncate(n int) error

	// EncodeMessage encodes an unaggregated message.
	EncodeMessage(msg encoding.UnaggregatedMessageUnion) error

	// Relinquish relinquishes ownership of the encoded byte stream to the caller,
	// and resets the internal encoding buffer.
	Relinquish() Buffer
}

type unaggregatedEncoder struct {
	pool           pool.BytesPool
	initBufSize    int
	maxMessageSize int

	cm   metricpb.CounterWithMetadatas
	bm   metricpb.BatchTimerWithMetadatas
	gm   metricpb.GaugeWithMetadatas
	fm   metricpb.ForwardedMetricWithMetadata
	tm   metricpb.TimedMetricWithMetadata
	tms  metricpb.TimedMetricWithMetadatas
	pm   metricpb.TimedMetricWithStoragePolicy
	buf  []byte
	used int

	encodeMessageSizeFn func(int)
	encodeMessageFn     func(metricpb.MetricWithMetadatas) error
}

// NewUnaggregatedEncoder creates a new unaggregated encoder.
func NewUnaggregatedEncoder(opts UnaggregatedOptions) UnaggregatedEncoder {
	e := &unaggregatedEncoder{
		pool:           opts.BytesPool(),
		initBufSize:    opts.InitBufferSize(),
		maxMessageSize: opts.MaxMessageSize(),
	}
	e.encodeMessageSizeFn = e.encodeMessageSize
	e.encodeMessageFn = e.encodeMessage
	e.Reset(nil)
	return e
}

func (enc *unaggregatedEncoder) Len() int { return enc.used }

func (enc *unaggregatedEncoder) Reset(initData []byte) {
	if enc.buf != nil && enc.pool != nil {
		enc.pool.Put(enc.buf)
	}
	bufSize := int(math.Max(float64(enc.initBufSize), float64(len(initData))))
	enc.buf = allocate(enc.pool, bufSize)
	copy(enc.buf, initData)
	enc.used = len(initData)
}

func (enc *unaggregatedEncoder) Truncate(n int) error {
	if n < 0 || n > enc.used {
		return fmt.Errorf("truncation out of range: used=%d, target=%d", enc.used, n)
	}
	enc.used = n
	return nil
}

func (enc *unaggregatedEncoder) Relinquish() Buffer {
	res := NewBuffer(enc.buf[:enc.used], enc.pool.Put)
	enc.buf = nil
	enc.used = 0
	return res
}

func (enc *unaggregatedEncoder) EncodeMessage(msg encoding.UnaggregatedMessageUnion) error {
	switch msg.Type {
	case encoding.CounterWithMetadatasType:
		return enc.encodeCounterWithMetadatas(msg.CounterWithMetadatas)
	case encoding.BatchTimerWithMetadatasType:
		return enc.encodeBatchTimerWithMetadatas(msg.BatchTimerWithMetadatas)
	case encoding.GaugeWithMetadatasType:
		return enc.encodeGaugeWithMetadatas(msg.GaugeWithMetadatas)
	case encoding.ForwardedMetricWithMetadataType:
		return enc.encodeForwardedMetricWithMetadata(msg.ForwardedMetricWithMetadata)
	case encoding.TimedMetricWithMetadataType:
		return enc.encodeTimedMetricWithMetadata(msg.TimedMetricWithMetadata)
	case encoding.TimedMetricWithMetadatasType:
		return enc.encodeTimedMetricWithMetadatas(msg.TimedMetricWithMetadatas)
	case encoding.PassthroughMetricWithMetadataType:
		return enc.encodePassthroughMetricWithMetadata(msg.PassthroughMetricWithMetadata)
	default:
		return fmt.Errorf("unknown message type: %v", msg.Type)
	}
}

func (enc *unaggregatedEncoder) encodeCounterWithMetadatas(cm unaggregated.CounterWithMetadatas) error {
	if err := cm.ToProto(&enc.cm); err != nil {
		return fmt.Errorf("counter with metadatas proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                 metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
		CounterWithMetadatas: &enc.cm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeBatchTimerWithMetadatas(bm unaggregated.BatchTimerWithMetadatas) error {
	if err := bm.ToProto(&enc.bm); err != nil {
		return fmt.Errorf("batch timer with metadatas proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                    metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
		BatchTimerWithMetadatas: &enc.bm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeGaugeWithMetadatas(gm unaggregated.GaugeWithMetadatas) error {
	if err := gm.ToProto(&enc.gm); err != nil {
		return fmt.Errorf("gauge with metadatas proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:               metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
		GaugeWithMetadatas: &enc.gm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeForwardedMetricWithMetadata(fm aggregated.ForwardedMetricWithMetadata) error {
	if err := fm.ToProto(&enc.fm); err != nil {
		return fmt.Errorf("forwarded metric with metadata proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                        metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
		ForwardedMetricWithMetadata: &enc.fm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeTimedMetricWithMetadata(tm aggregated.TimedMetricWithMetadata) error {
	if err := tm.ToProto(&enc.tm); err != nil {
		return fmt.Errorf("timed metric with metadata proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                    metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA,
		TimedMetricWithMetadata: &enc.tm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeTimedMetricWithMetadatas(tms aggregated.TimedMetricWithMetadatas) error {
	if err := tms.ToProto(&enc.tms); err != nil {
		return fmt.Errorf("timed metric with metadatas proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                     metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS,
		TimedMetricWithMetadatas: &enc.tms,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodePassthroughMetricWithMetadata(pm aggregated.PassthroughMetricWithMetadata) error {
	if err := pm.ToProto(&enc.pm); err != nil {
		return fmt.Errorf("passthrough metric with metadata proto conversion failed: %v", err)
	}
	mm := metricpb.MetricWithMetadatas{
		Type:                         metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY,
		TimedMetricWithStoragePolicy: &enc.pm,
	}
	return enc.encodeMetricWithMetadatas(mm)
}

func (enc *unaggregatedEncoder) encodeMetricWithMetadatas(pb metricpb.MetricWithMetadatas) error {
	msgSize := pb.Size()
	if msgSize > enc.maxMessageSize {
		return fmt.Errorf("message size %d is larger than maximum supported size %d", msgSize, enc.maxMessageSize)
	}
	payloadSize := maxMessageSizeInBytes + msgSize
	enc.ensureBufferSize(enc.used + payloadSize)
	enc.encodeMessageSizeFn(msgSize)
	return enc.encodeMessageFn(pb)
}

// ensureBufferSize ensures the internal buffer has at least the specified target size.
func (enc *unaggregatedEncoder) ensureBufferSize(targetSize int) {
	enc.buf = ensureBufferSize(enc.buf, enc.pool, targetSize, copyData)
}

func (enc *unaggregatedEncoder) encodeMessageSize(msgSize int) {
	n := binary.PutVarint(enc.buf[enc.used:], int64(msgSize))
	enc.used += n
}

func (enc *unaggregatedEncoder) encodeMessage(pb metricpb.MetricWithMetadatas) error {
	n, err := pb.MarshalTo(enc.buf[enc.used:])
	if err != nil {
		return err
	}
	enc.used += n
	return nil
}
