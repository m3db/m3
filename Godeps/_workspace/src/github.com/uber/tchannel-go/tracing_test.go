// Copyright (c) 2015 Uber Technologies, Inc.

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

package tchannel_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/uber/tchannel-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/json"
	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"golang.org/x/net/context"
)

type TracingRequest struct {
	ForwardCount int
}

type TracingResponse struct {
	TraceID        uint64
	SpanID         uint64
	ParentID       uint64
	TracingEnabled bool
	Child          *TracingResponse
}

type traceHandler struct {
	ch *Channel
	t  *testing.T
}

func (h *traceHandler) call(ctx json.Context, req *TracingRequest) (*TracingResponse, error) {
	span := CurrentSpan(ctx)
	if span == nil {
		return nil, fmt.Errorf("tracing not found")
	}

	var childResp *TracingResponse
	if req.ForwardCount > 0 {
		sc := h.ch.Peers().GetOrAdd(h.ch.PeerInfo().HostPort)
		childResp = new(TracingResponse)
		require.NoError(h.t, json.CallPeer(ctx, sc, h.ch.PeerInfo().ServiceName, "call", nil, childResp))
	}

	return &TracingResponse{
		TraceID:        span.TraceID(),
		SpanID:         span.SpanID(),
		ParentID:       span.ParentID(),
		TracingEnabled: span.TracingEnabled(),
		Child:          childResp,
	}, nil
}

func (h *traceHandler) onError(ctx context.Context, err error) {
	h.t.Errorf("onError %v", err)
}

func TestTracingPropagates(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		handler := &traceHandler{t: t, ch: ch}
		json.Register(ch, json.Handlers{
			"call": handler.call,
		}, handler.onError)

		ctx, cancel := json.NewContext(time.Second)
		defer cancel()

		peer := ch.Peers().GetOrAdd(ch.PeerInfo().HostPort)
		var response TracingResponse
		require.NoError(t, json.CallPeer(ctx, peer, ch.PeerInfo().ServiceName, "call", &TracingRequest{
			ForwardCount: 1,
		}, &response))

		clientSpan := CurrentSpan(ctx)
		require.NotNil(t, clientSpan)
		assert.Equal(t, uint64(0), clientSpan.ParentID())
		assert.NotEqual(t, uint64(0), clientSpan.TraceID())
		assert.True(t, clientSpan.TracingEnabled(), "Tracing should be enabled")
		assert.Equal(t, clientSpan.TraceID(), response.TraceID)
		assert.Equal(t, clientSpan.SpanID(), response.ParentID)
		assert.True(t, response.TracingEnabled, "Tracing should be enabled")
		assert.Equal(t, response.TraceID, response.SpanID, "traceID = spanID for root span")

		nestedResponse := response.Child
		require.NotNil(t, nestedResponse)
		assert.Equal(t, clientSpan.TraceID(), nestedResponse.TraceID)
		assert.Equal(t, response.SpanID, nestedResponse.ParentID)
		assert.True(t, response.TracingEnabled, "Tracing should be enabled")
		assert.NotEqual(t, response.SpanID, nestedResponse.SpanID)
	})
}

func TestTraceReportingEnabled(t *testing.T) {
	initialTime := time.Date(2015, 2, 1, 10, 10, 0, 0, time.UTC)

	var state struct {
		signal chan struct{}

		call TraceData
		span Span
	}
	testTraceReporter := TraceReporterFunc(func(data TraceData) {
		defer close(state.signal)

		span := data.Span
		data.Span = Span{}
		state.call = data
		state.span = span
	})

	traceReporterOpts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
	tests := []struct {
		name       string
		serverOpts *testutils.ChannelOpts
		clientOpts *testutils.ChannelOpts
		expected   []Annotation
		fromServer bool
	}{
		{
			name:       "inbound",
			serverOpts: traceReporterOpts,
			expected: []Annotation{
				{Key: "sr", Timestamp: initialTime.Add(2 * time.Second)},
				{Key: "ss", Timestamp: initialTime.Add(3 * time.Second)},
			},
			fromServer: true,
		},
		{
			name:       "outbound",
			clientOpts: traceReporterOpts,
			expected: []Annotation{
				{Key: "cs", Timestamp: initialTime.Add(time.Second)},
				{Key: "cr", Timestamp: initialTime.Add(6 * time.Second)},
			},
		},
	}

	for _, tt := range tests {
		state.signal = make(chan struct{})

		serverNow, serverNowFn := testutils.NowStub(initialTime.Add(time.Second))
		clientNow, clientNowFn := testutils.NowStub(initialTime)
		serverNowFn(time.Second)
		clientNowFn(time.Second)

		tt.serverOpts = testutils.DefaultOpts(tt.serverOpts).SetTimeNow(serverNow)
		tt.clientOpts = testutils.DefaultOpts(tt.clientOpts).SetTimeNow(clientNow)

		WithVerifiedServer(t, tt.serverOpts, func(ch *Channel, hostPort string) {
			testutils.RegisterEcho(ch, func() {
				clientNowFn(5 * time.Second)
			})

			clientCh := testutils.NewClient(t, tt.clientOpts)
			defer clientCh.Close()
			ctx, cancel := NewContext(time.Second)
			defer cancel()

			_, _, _, err := raw.Call(ctx, clientCh, hostPort, ch.PeerInfo().ServiceName, "echo", nil, []byte("arg3"))
			require.NoError(t, err, "raw.Call failed")

			binaryAnnotations := []BinaryAnnotation{
				{"cn", clientCh.PeerInfo().ServiceName},
				{"as", Raw.String()},
			}
			target := TraceEndpoint{
				HostPort:    hostPort,
				ServiceName: ch.ServiceName(),
			}
			source := target
			if !tt.fromServer {
				source = TraceEndpoint{
					HostPort:    "0.0.0.0:0",
					ServiceName: clientCh.ServiceName(),
				}
			}

			select {
			case <-state.signal:
			case <-time.After(time.Second):
				t.Fatalf("Did not receive trace report within timeout")
			}

			expected := TraceData{Annotations: tt.expected, BinaryAnnotations: binaryAnnotations, Source: source, Target: target, Method: "echo"}
			assert.Equal(t, expected, state.call, "%v: Report args mismatch", tt.name)
			curSpan := CurrentSpan(ctx)
			assert.Equal(t, NewSpan(curSpan.TraceID(), curSpan.TraceID(), 0), state.span, "Span mismatch")
		})
	}
}

func TestTraceReportingDisabled(t *testing.T) {
	var gotCalls int
	testTraceReporter := TraceReporterFunc(func(_ TraceData) {
		gotCalls++
	})

	traceReporterOpts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
	WithVerifiedServer(t, traceReporterOpts, func(ch *Channel, hostPort string) {
		ch.Register(raw.Wrap(newTestHandler(t)), "echo")

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		CurrentSpan(ctx).EnableTracing(false)
		_, _, _, err := raw.Call(ctx, ch, hostPort, ch.PeerInfo().ServiceName, "echo", nil, []byte("arg3"))
		require.NoError(t, err, "raw.Call failed")

		assert.Equal(t, 0, gotCalls, "TraceReporter should not report if disabled")
	})
}

func TestTraceSamplingRate(t *testing.T) {
	rand.Seed(10)

	tests := []struct {
		sampleRate  float64 // if this is < 0, then the value is not set.
		count       int
		expectedMin int
		expectedMax int
	}{
		{1.0, 100, 100, 100},
		{0.5, 100, 40, 60},
		{0.1, 100, 5, 15},
		{0, 100, 0, 0},
		{-1, 100, 100, 100}, // default of 1.0 should be used.
	}

	for _, tt := range tests {
		var reportedTraces int
		testTraceReporter := TraceReporterFunc(func(_ TraceData) {
			reportedTraces++
		})

		WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
			var tracedCalls int
			testutils.RegisterFunc(ch, "t", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
				if CurrentSpan(ctx).TracingEnabled() {
					tracedCalls++
				}

				return &raw.Res{}, nil
			})

			opts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
			if tt.sampleRate >= 0 {
				opts.SetTraceSampleRate(tt.sampleRate)
			}

			client := testutils.NewClient(t, opts)
			defer client.Close()

			for i := 0; i < tt.count; i++ {
				ctx, cancel := NewContext(time.Second)
				defer cancel()

				_, _, _, err := raw.Call(ctx, client, hostPort, ch.PeerInfo().ServiceName, "t", nil, nil)
				require.NoError(t, err, "raw.Call failed")
			}

			assert.Equal(t, reportedTraces, tracedCalls,
				"Number of traces report doesn't match calls with tracing enabled")
			assert.True(t, tracedCalls >= tt.expectedMin,
				"Number of trace enabled calls (%v) expected to be greater than %v", tracedCalls, tt.expectedMin)
			assert.True(t, tracedCalls <= tt.expectedMax,
				"Number of trace enabled calls (%v) expected to be less than %v", tracedCalls, tt.expectedMax)
		})
	}
}

func TestChildCallsNotSampled(t *testing.T) {
	var traceEnabledCalls int

	s1 := testutils.NewServer(t, testutils.NewOpts().SetTraceSampleRate(0.0001))
	defer s1.Close()
	s2 := testutils.NewServer(t, nil)
	defer s2.Close()

	testutils.RegisterFunc(s1, "s1", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		_, _, _, err := raw.Call(ctx, s1, s2.PeerInfo().HostPort, s2.ServiceName(), "s2", nil, nil)
		require.NoError(t, err, "raw.Call from s1 to s2 failed")
		return &raw.Res{}, nil
	})

	testutils.RegisterFunc(s2, "s2", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		if CurrentSpan(ctx).TracingEnabled() {
			traceEnabledCalls++
		}
		return &raw.Res{}, nil
	})

	client := testutils.NewClient(t, nil)
	defer client.Close()

	const numCalls = 100
	for i := 0; i < numCalls; i++ {
		ctx, cancel := NewContext(time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, client, s1.PeerInfo().HostPort, s1.ServiceName(), "s1", nil, nil)
		require.NoError(t, err, "raw.Call to s1 failed")
	}

	// Even though s1 has sampling enabled, it should not affect incoming calls.
	assert.Equal(t, numCalls, traceEnabledCalls, "Trace sampling should not inbound calls")
}
