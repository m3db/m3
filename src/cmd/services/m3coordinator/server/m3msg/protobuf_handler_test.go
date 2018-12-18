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
	"net"
	"testing"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/server"

	"github.com/stretchr/testify/require"
)

var testID = "stats.sjc1.gauges.m3+some-name+dc=sjc1,env=production,service=foo,type=gauge"

func TestM3msgServerWithProtobufHandler(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	w := &mockWriter{m: make(map[string]payload)}
	hOpts := Options{
		WriteFn:           w.write,
		InstrumentOptions: instrument.NewOptions(),
	}
	handler := newProtobufHandler(hOpts)
	require.NoError(t, err)

	opts := consumer.NewOptions().
		SetAckBufferSize(1).
		SetConnectionWriteBufferSize(1)

	s := server.NewServer(
		"a",
		consumer.NewMessageHandler(handler.message, opts),
		server.NewOptions(),
	)
	s.Serve(l)

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	m := aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        []byte(testID),
			TimeNanos: 1000,
			Value:     1,
			Type:      metric.GaugeType,
		},
		StoragePolicy: validStoragePolicy,
	}

	encoder := protobuf.NewAggregatedEncoder(nil)
	require.NoError(t, encoder.Encode(m, 2000))
	enc := proto.NewEncoder(opts.EncoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Buffer().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)

	var a msgpb.Ack
	dec := proto.NewDecoder(conn, opts.DecoderOptions())
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 1, w.ingested())

	payload, ok := w.m[string(m.ID)]
	require.True(t, ok)
	require.Equal(t, string(m.ID), payload.id)
	require.Equal(t, m.TimeNanos, payload.metricTime.UnixNano())
	require.Equal(t, m.Value, payload.value)
	require.Equal(t, m.StoragePolicy, payload.sp)
}
