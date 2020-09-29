// Copyright (c) 2020 Uber Technologies, Inc.
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

package grpc

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	grpcTypeUnary        = "unary"
	grpcTypeClientStream = "client_stream"
	grpcTypeServerStream = "server_stream"
	grpcTypeBidiStream   = "bidi_stream"
)

// InterceptorInstrumentOptions is a set of options for instrumented interceptors.
type InterceptorInstrumentOptions struct {
	// Scope, required.
	Scope tally.Scope
	// TimerOptions, optional and if not set will use defaults.
	TimerOptions *instrument.TimerOptions
}

type interceptorInstrumentOptions struct {
	Scope        tally.Scope
	TimerOptions instrument.TimerOptions
}

func (o InterceptorInstrumentOptions) resolve() interceptorInstrumentOptions {
	result := interceptorInstrumentOptions{Scope: o.Scope}
	if o.TimerOptions == nil {
		result.TimerOptions = DefaultTimerOptions()
	} else {
		result.TimerOptions = *o.TimerOptions
	}
	return result
}

// UnaryClientInterceptor provides tally metrics for client unary calls.
func UnaryClientInterceptor(
	opts InterceptorInstrumentOptions,
) func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	resolvedOpts := opts.resolve()
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		m := newClientMetrics(grpcTypeUnary, method, resolvedOpts)
		err := invoker(ctx, method, req, reply, cc, opts...)
		st, _ := status.FromError(err)
		m.Handled(st.Code())
		return err
	}
}

// StreamClientInterceptor provides tally metrics for client streams.
func StreamClientInterceptor(
	opts InterceptorInstrumentOptions,
) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	resolvedOpts := opts.resolve()
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		m := newClientMetrics(rpcTypeFromStreamDesc(desc), method, resolvedOpts)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			st, _ := status.FromError(err)
			m.Handled(st.Code())
			return nil, err
		}
		return &monitoredClientStream{ClientStream: stream, metrics: m}, nil
	}
}

type monitoredClientStream struct {
	grpc.ClientStream
	metrics clientMetrics
}

func (s *monitoredClientStream) SendMsg(m interface{}) error {
	timer := s.metrics.SendMessageTimer()
	err := s.ClientStream.SendMsg(m)
	timer.Stop()
	if err == nil {
		s.metrics.SentMessage()
	}
	return err
}

func (s *monitoredClientStream) RecvMsg(m interface{}) error {
	timer := s.metrics.ReceiveMessageTimer()
	err := s.ClientStream.RecvMsg(m)
	timer.Stop()

	if err == nil {
		s.metrics.ReceivedMessage()
	} else if err == io.EOF {
		s.metrics.Handled(codes.OK)
	} else {
		st, _ := status.FromError(err)
		s.metrics.Handled(st.Code())
	}
	return err
}

type clientMetrics struct {
	scope                     tally.Scope
	startTime                 time.Time
	tags                      map[string]string
	clientStartedCounter      tally.Counter
	clientHandledHistogram    tally.Timer
	clientStreamRecvHistogram tally.Timer
	clientStreamMsgReceived   tally.Counter
	clientStreamSendHistogram tally.Timer
	clientStreamMsgSent       tally.Counter
}

func newClientMetrics(
	rpcType string,
	fullMethod string,
	opts interceptorInstrumentOptions,
) clientMetrics {
	var (
		name            = strings.TrimPrefix(fullMethod, "/")
		service, method = "unknown", "unknown"
	)
	if i := strings.Index(name, "/"); i >= 0 {
		service, method = name[:i], name[i+1:]
	}

	tags := map[string]string{
		"grpc_type":    rpcType,
		"grpc_service": service,
		"grpc_method":  method,
	}

	scope := opts.Scope.SubScope("grpc").Tagged(tags)

	m := clientMetrics{
		scope:                scope,
		startTime:            time.Now(),
		tags:                 tags, // Reuse tags for later subscoping.
		clientStartedCounter: scope.Counter("client_started_total"),
		clientHandledHistogram: instrument.NewTimer(scope,
			"client_handling_seconds", opts.TimerOptions),
		clientStreamRecvHistogram: instrument.NewTimer(scope,
			"client_msg_recv_handling_seconds", opts.TimerOptions),
		clientStreamMsgReceived: scope.Counter("client_msg_received_total"),
		clientStreamSendHistogram: instrument.NewTimer(scope,
			"client_msg_send_handling_seconds", opts.TimerOptions),
		clientStreamMsgSent: scope.Counter("client_msg_sent_total"),
	}
	m.clientStartedCounter.Inc(1)
	return m
}

func (m clientMetrics) ReceiveMessageTimer() tally.Stopwatch {
	return m.clientStreamRecvHistogram.Start()
}

func (m clientMetrics) ReceivedMessage() {
	m.clientStreamMsgReceived.Inc(1)
}

func (m clientMetrics) SendMessageTimer() tally.Stopwatch {
	return m.clientStreamSendHistogram.Start()
}

func (m clientMetrics) SentMessage() {
	m.clientStreamMsgSent.Inc(1)
}

func (m clientMetrics) Handled(code codes.Code) {
	// Reuse tags map.
	for k := range m.tags {
		delete(m.tags, k)
	}
	m.tags["grpc_code"] = code.String()
	subscope := m.scope.Tagged(m.tags)
	subscope.Counter("client_handled_total").Inc(1)
	m.clientHandledHistogram.Record(time.Since(m.startTime))
}

func rpcTypeFromStreamDesc(desc *grpc.StreamDesc) string {
	if desc.ClientStreams && !desc.ServerStreams {
		return grpcTypeClientStream
	} else if !desc.ClientStreams && desc.ServerStreams {
		return grpcTypeServerStream
	}
	return grpcTypeBidiStream
}

// DefaultTimerOptions returns a sane default timer options with buckets from
// 1ms to 10mins.
func DefaultTimerOptions() instrument.TimerOptions {
	return instrument.NewHistogramTimerOptions(instrument.HistogramTimerOptions{
		HistogramBuckets: tally.ValueBuckets{
			0,
			0.001,
			0.002,
			0.003,
			0.004,
			0.005,
			0.006,
			0.007,
			0.008,
			0.009,
			0.01,
			0.02,
			0.04,
			0.06,
			0.08,
			0.1,
			0.2,
			0.4,
			0.6,
			0.8,
			1,
			1.5,
			2,
			2.5,
			3,
			3.5,
			4,
			4.5,
			5,
			5.5,
			6,
			6.5,
			7,
			7.5,
			8,
			8.5,
			9,
			9.5,
			10,
			15,
			20,
			25,
			30,
			35,
			40,
			45,
			50,
			55,
			60,
			150,
			300,
			450,
			600,
		},
	})
}
