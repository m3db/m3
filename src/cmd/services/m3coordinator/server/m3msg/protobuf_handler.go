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

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Options for the ingest handler.
type Options struct {
	InstrumentOptions          instrument.Options
	WriteFn                    WriteFn
	ProtobufDecoderPoolOptions pool.ObjectPoolOptions
	BlockholePolicies          []policy.StoragePolicy
}

type handlerMetrics struct {
	messageReadError             tally.Counter
	metricAccepted               tally.Counter
	droppedMetricBlackholePolicy tally.Counter
	droppedMetricDecodeError     tally.Counter
}

func newHandlerMetrics(scope tally.Scope) handlerMetrics {
	messageScope := scope.SubScope("metric")
	return handlerMetrics{
		messageReadError: scope.Counter("message-read-error"),
		metricAccepted:   messageScope.Counter("accepted"),
		droppedMetricDecodeError: messageScope.Tagged(map[string]string{
			"reason": "decode-error",
		}).Counter("dropped"),
		droppedMetricBlackholePolicy: messageScope.Tagged(map[string]string{
			"reason": "blackhole-policy",
		}).Counter("dropped"),
	}
}

type pbHandler struct {
	ctx     context.Context
	writeFn WriteFn
	pool    protobuf.AggregatedDecoderPool
	wg      *sync.WaitGroup
	logger  *zap.Logger
	m       handlerMetrics

	// Set of policies for which when we see a metric we drop it on the floor.
	blackholePolicies []policy.StoragePolicy
}

func newProtobufProcessor(opts Options) consumer.MessageProcessor {
	p := protobuf.NewAggregatedDecoderPool(opts.ProtobufDecoderPoolOptions)
	p.Init()

	h := &pbHandler{
		ctx:               context.Background(),
		writeFn:           opts.WriteFn,
		pool:              p,
		wg:                &sync.WaitGroup{},
		logger:            opts.InstrumentOptions.Logger(),
		m:                 newHandlerMetrics(opts.InstrumentOptions.MetricsScope()),
		blackholePolicies: opts.BlockholePolicies,
	}

	if len(opts.BlockholePolicies) > 0 {
		policyNames := make([]string, 0, len(opts.BlockholePolicies))
		for _, sp := range h.blackholePolicies {
			policyNames = append(policyNames, sp.String())
		}
		h.logger.Info("m3msg handler blackholing metrics for configured policies", zap.Strings("policyNames", policyNames))
	}

	return h
}

func (h *pbHandler) Process(msg consumer.Message) {
	dec := h.pool.Get()
	if err := dec.Decode(msg.Bytes()); err != nil {
		h.logger.Error("could not decode metric from message", zap.Error(err))
		h.m.droppedMetricDecodeError.Inc(1)
		return
	}
	h.m.metricAccepted.Inc(1)

	h.wg.Add(1)
	r := NewProtobufCallback(msg, dec, h.wg)
	sp := dec.StoragePolicy()
	// If storage policy is blackholed, ack the message immediately and don't
	// bother passing down the write path.
	for _, blackholeSp := range h.blackholePolicies {
		if sp.Equivalent(blackholeSp) {
			h.m.droppedMetricBlackholePolicy.Inc(1)
			r.Callback(OnSuccess)
			return
		}
	}

	h.writeFn(h.ctx, dec.ID(), dec.Type(), dec.TimeNanos(), dec.EncodeNanos(), dec.Value(), sp, r)
}

func (h *pbHandler) Close() { h.wg.Wait() }

type protobufCallback struct {
	msg consumer.Message
	dec *protobuf.AggregatedDecoder
	wg  *sync.WaitGroup
}

// NewProtobufCallback creates a callbackable.
func NewProtobufCallback(
	msg consumer.Message,
	dec *protobuf.AggregatedDecoder,
	wg *sync.WaitGroup,
) Callbackable {
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
	// Close the decoder, returns the underlying bytes to the pool.
	c.dec.Close()
}
