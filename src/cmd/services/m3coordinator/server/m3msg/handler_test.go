package m3msg

import (
	"net"
	"sync"
	"testing"

	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3msg/consumer"
	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/protocol/proto"
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

	m       map[string]payload
	enabled bool
	n       int
}

func (m *mockWriter) write(
	name []byte,
	metricTime int64,
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
	metricTime int64
	value      float64
	sp         policy.StoragePolicy
}
