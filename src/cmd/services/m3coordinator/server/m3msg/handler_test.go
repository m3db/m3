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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/msgpack"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

var (
	validStoragePolicy = policy.MustParseStoragePolicy("1m:40d")
)

func TestM3msgServerHandlerWithMultipleMetricsPerMessage(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	m := &mockWriter{m: make(map[string]payload)}
	hOpts := Options{
		WriteFn:           m.write,
		InstrumentOptions: instrument.NewOptions(),
	}
	handler, err := newHandler(hOpts)
	require.NoError(t, err)
	sOpts := consumer.NewServerOptions().
		SetConsumerOptions(
			consumer.NewOptions().
				SetAckBufferSize(1).
				SetConnectionWriteBufferSize(1),
		).
		SetConsumeFn(handler.Handle)
	s := consumer.NewServer("a", sOpts)
	s.Serve(l)

	encoder := msgpack.NewAggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil))
	chunkedMetricWithPolicy := aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: id.ChunkedID{
				Data: []byte("stats.sjc1.gauges.m3+some-name+dc=sjc1,env=production,service=foo,type=gauge"),
			},
			TimeNanos: 1000,
			Value:     1,
		},
		StoragePolicy: validStoragePolicy,
	}
	require.NoError(t, encoder.EncodeChunkedMetricWithStoragePolicyAndEncodeTime(chunkedMetricWithPolicy, 2000))
	require.NoError(t, encoder.EncodeChunkedMetricWithStoragePolicyAndEncodeTime(chunkedMetricWithPolicy, 3000))

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	enc := proto.NewEncoder(sOpts.ConsumerOptions().EncoderOptions())
	dec := proto.NewDecoder(conn, sOpts.ConsumerOptions().DecoderOptions())
	require.NoError(t, enc.Encode(&msgpb.Message{
		Value: encoder.Encoder().Bytes(),
	}))
	_, err = conn.Write(enc.Bytes())
	require.NoError(t, err)

	var a msgpb.Ack
	require.NoError(t, dec.Decode(&a))
	require.Equal(t, 2, m.ingested())
}

type mockWriter struct {
	sync.Mutex

	m map[string]payload
	n int
}

func (m *mockWriter) write(
	ctx context.Context,
	name []byte,
	metricTime time.Time,
	value float64,
	sp policy.StoragePolicy,
	callbackable *RefCountedCallback,
) {
	m.Lock()
	m.n++
	payload := payload{
		id:         string(name),
		metricTime: metricTime,
		value:      value,
		sp:         sp,
	}
	m.m[payload.id] = payload
	m.Unlock()
	callbackable.Callback(OnSuccess)
}

func (m *mockWriter) ingested() int {
	m.Lock()
	defer m.Unlock()

	return m.n
}

type payload struct {
	id         string
	metricTime time.Time
	value      float64
	sp         policy.StoragePolicy
}
