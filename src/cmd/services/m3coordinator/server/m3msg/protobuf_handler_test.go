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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/server"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testID = "stats.foo1.gauges.m3+some-name+dc=foo1,env=production,service=foo,type=gauge"

	// baseStoragePolicy represents what we typically define in config for SP.
	// precisionStoragePolicy is the same retention/resolution, but includes the
	// precision (which is often included with incoming writes).
	baseStoragePolicy      = policy.MustParseStoragePolicy("1m:40d")
	precisionStoragePolicy = policy.NewStoragePolicy(time.Minute, xtime.Second, 40*24*time.Hour)
)

func TestM3MsgServerWithProtobufHandler(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	w := &mockWriter{m: make(map[string]payload)}
	hOpts := Options{
		WriteFn:           w.write,
		InstrumentOptions: instrument.NewOptions(),
	}
	opts := consumer.NewOptions().
		SetAckBufferSize(1).
		SetConnectionWriteBufferSize(1)

	s := server.NewServer(
		"a",
		consumer.NewMessageHandler(newProtobufProcessor(hOpts), opts),
		server.NewOptions(),
	)
	s.Serve(l)

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	m1 := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte(testID),
			TimeNanos: 1000,
			Value:     1,
			Type:      metric.GaugeType,
		},
		StoragePolicy: precisionStoragePolicy,
	}

	encoder := protobuf.NewAggregatedEncoder(nil)
	require.NoError(t, encoder.Encode(m1, 2000))
	enc := proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)

	var a msgpb.Ack
	dec := proto.NewDecoder(conn, opts.DecoderOptions(), 10)
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 1, w.ingested())

	m2 := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte{},
			TimeNanos: 0,
			Value:     0,
			Type:      metric.UnknownType,
		},
		StoragePolicy: precisionStoragePolicy,
	}
	require.NoError(t, encoder.Encode(m2, 3000))
	enc = proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 2, w.ingested())

	payload, ok := w.m[key(string(m1.ID), 2000)]
	require.True(t, ok)
	require.Equal(t, string(m1.ID), payload.id)
	require.Equal(t, m1.TimeNanos, payload.metricNanos)
	require.Equal(t, 2000, int(payload.encodeNanos))
	require.Equal(t, m1.Value, payload.value)
	require.Equal(t, m1.StoragePolicy, payload.sp)

	payload, ok = w.m[key(string(m2.ID), 3000)]
	require.True(t, ok)
	require.Equal(t, string(m2.ID), payload.id)
	require.Equal(t, m2.TimeNanos, payload.metricNanos)
	require.Equal(t, 3000, int(payload.encodeNanos))
	require.Equal(t, m2.Value, payload.value)
	require.Equal(t, m2.StoragePolicy, payload.sp)
}

func TestM3MsgServerWithProtobufHandler_Blackhole(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	w := &mockWriter{m: make(map[string]payload)}
	hOpts := Options{
		WriteFn:           w.write,
		InstrumentOptions: instrument.NewOptions(),
		BlockholePolicies: []policy.StoragePolicy{baseStoragePolicy},
	}
	opts := consumer.NewOptions().
		SetAckBufferSize(1).
		SetConnectionWriteBufferSize(1)

	s := server.NewServer(
		"a",
		consumer.NewMessageHandler(newProtobufProcessor(hOpts), opts),
		server.NewOptions(),
	)
	s.Serve(l)

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	m1 := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte(testID),
			TimeNanos: 1000,
			Value:     1,
			Type:      metric.GaugeType,
		},
		StoragePolicy: precisionStoragePolicy,
	}

	encoder := protobuf.NewAggregatedEncoder(nil)
	require.NoError(t, encoder.Encode(m1, 2000))
	enc := proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)

	var a msgpb.Ack
	dec := proto.NewDecoder(conn, opts.DecoderOptions(), 10)
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 0, w.ingested())

	// Ensure a metric with a different policy still gets ingested.
	m2 := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte{},
			TimeNanos: 0,
			Value:     0,
			Type:      metric.UnknownType,
		},
		StoragePolicy: policy.MustParseStoragePolicy("5m:180d"),
	}
	require.NoError(t, encoder.Encode(m2, 3000))
	enc = proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 1, w.ingested())

	// Ensure a metric with base policy (equivalent but default precision) is
	// still ignored.
	m3 := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte(testID),
			TimeNanos: 1000,
			Value:     1,
			Type:      metric.GaugeType,
		},
		StoragePolicy: baseStoragePolicy,
	}
	require.NoError(t, encoder.Encode(m3, 3000))
	enc = proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 1, w.ingested())
}

type mockWriter struct {
	sync.Mutex

	m map[string]payload
	n int
}

func (m *mockWriter) write(
	ctx context.Context,
	name []byte,
	metricType ts.PromMetricType,
	metricNanos, encodeNanos int64,
	value float64,
	sp policy.StoragePolicy,
	callbackable Callbackable,
) {
	m.Lock()
	m.n++
	payload := payload{
		id:          string(name),
		metricNanos: metricNanos,
		encodeNanos: encodeNanos,
		value:       value,
		sp:          sp,
	}
	m.m[key(payload.id, encodeNanos)] = payload
	m.Unlock()
	callbackable.Callback(OnSuccess)
}

func (m *mockWriter) ingested() int {
	m.Lock()
	defer m.Unlock()

	return m.n
}

func key(id string, encodeTime int64) string {
	return fmt.Sprintf("%s%d", id, encodeTime)
}

type payload struct {
	id          string
	metricNanos int64
	encodeNanos int64
	value       float64
	sp          policy.StoragePolicy
}
