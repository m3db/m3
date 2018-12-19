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

package m3msg

import (
	"context"
	"sync"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3x/log"
)

type pbHandler struct {
	ctx     context.Context
	writeFn WriteFn
	pool    protobuf.AggregatedDecoderPool
	wg      *sync.WaitGroup
	logger  log.Logger
	m       handlerMetrics
}

func newProtobufProcessor(opts Options) consumer.MessageProcessor {
	p := protobuf.NewAggregatedDecoderPool(opts.ProtobufDecoderPoolOptions)
	p.Init()
	return &pbHandler{
		ctx:     context.Background(),
		writeFn: opts.WriteFn,
		pool:    p,
		wg:      &sync.WaitGroup{},
		logger:  opts.InstrumentOptions.Logger(),
		m:       newHandlerMetrics(opts.InstrumentOptions.MetricsScope()),
	}
}

func (h *pbHandler) Process(msg consumer.Message) {
	dec := h.pool.Get()
	if err := dec.Decode(msg.Bytes()); err != nil {
		h.logger.WithFields(log.NewErrField(err)).Error("invalid raw metric")
		h.m.droppedMetricDecodeError.Inc(1)
		return
	}
	sp, err := dec.StoragePolicy()
	if err != nil {
		h.logger.WithFields(log.NewErrField(err)).Error("invalid storage policy")
		h.m.droppedMetricDecodeMalformed.Inc(1)
		return
	}
	h.m.metricAccepted.Inc(1)

	var encodeTime time.Time
	if encodeNanos := dec.EncodeNanos(); encodeNanos > 0 {
		encodeTime = time.Unix(0, encodeNanos)
	}
	h.wg.Add(1)
	r := newProtobufCallback(msg, dec, h.wg)
	h.writeFn(h.ctx, dec.ID(), time.Unix(0, dec.TimeNanos()), encodeTime, dec.Value(), sp, r)
}

func (h *pbHandler) Close() { h.wg.Wait() }

type protobufCallback struct {
	msg consumer.Message
	dec *protobuf.AggregatedDecoder
	wg  *sync.WaitGroup
}

func newProtobufCallback(
	msg consumer.Message,
	dec *protobuf.AggregatedDecoder,
	wg *sync.WaitGroup,
) *protobufCallback {
	return &protobufCallback{
		msg: msg,
		dec: dec,
		wg:  wg,
	}
}

func (c *protobufCallback) Callback(t CallbackType) {
	switch t {
	case OnSuccess, OnNonRetriableError:
		c.msg.Ack()
	}
	c.wg.Done()
	c.dec.Close()
}
