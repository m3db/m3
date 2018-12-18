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
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3x/log"
)

type pbHandler struct {
	ctx     context.Context
	writeFn WriteFn
	pool    protobuf.AggregatedDecoderPool
	logger  log.Logger
	m       handlerMetrics
}

func newProtobufHandler(opts Options) *pbHandler {
	p := protobuf.NewAggregatedDecoderPool(opts.ProtobufDecoderPoolOptions)
	p.Init()
	return &pbHandler{
		ctx:     context.Background(),
		writeFn: opts.WriteFn,
		pool:    p,
		logger:  opts.InstrumentOptions.Logger(),
		m:       newHandlerMetrics(opts.InstrumentOptions.MetricsScope()),
	}
}

func (h *pbHandler) message(msg consumer.Message) {
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

	r := newProtobufCallback(msg, dec)
	h.writeFn(h.ctx, dec.ID(), time.Unix(0, dec.TimeNanos()), dec.Value(), sp, r)
}

type protobufCallback struct {
	msg consumer.Message
	dec *protobuf.AggregatedDecoder
}

func newProtobufCallback(
	msg consumer.Message,
	dec *protobuf.AggregatedDecoder,
) *protobufCallback {
	return &protobufCallback{
		msg: msg,
		dec: dec,
	}
}

func (c *protobufCallback) Callback(t CallbackType) {
	switch t {
	case OnSuccess, OnNonRetriableError:
		c.msg.Ack()
	}
	c.dec.Close()
}
