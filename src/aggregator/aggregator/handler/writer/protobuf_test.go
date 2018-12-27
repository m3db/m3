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

package writer

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestProtobufWriterWriteClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writer := testProtobufWriter(t, ctrl, NewOptions())
	writer.closed = true
	require.Equal(t, errWriterClosed, writer.Write(testChunkedMetricWithStoragePolicy))
}

func TestProtobufWriterWrite(t *testing.T) {
	now := time.Now()
	nowFn := func() time.Time { return now }
	opts := NewOptions().
		SetClockOptions(clock.NewOptions().SetNowFn(nowFn)).
		SetMaxBufferSize(math.MaxInt64).
		SetEncodingTimeSamplingRate(0.5)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	writer := testProtobufWriter(t, ctrl, opts)

	actualData := make(map[uint32][]decodeData)
	writer.p.(*producer.MockProducer).EXPECT().Produce(gomock.Any()).Do(func(m producer.Message) error {
		d := protobuf.NewAggregatedDecoder(nil)
		d.Decode(m.Bytes())
		s := m.Shard()
		sp, err := d.StoragePolicy()
		require.NoError(t, err)
		actualData[s] = append(actualData[s], decodeData{
			MetricWithStoragePolicy: aggregated.MetricWithStoragePolicy{
				Metric: aggregated.Metric{
					ID:        d.ID(),
					TimeNanos: d.TimeNanos(),
					Value:     d.Value(),
				},
				StoragePolicy: sp,
			},
			encodedAtNanos: d.EncodeNanos(),
		})
		return nil
	}).AnyTimes()
	writer.shardFn = func(id []byte) uint32 {
		if bytes.Equal(id, testRawID) {
			return 1
		}
		if bytes.Equal(id, testRawID2) {
			return 2
		}
		require.Fail(t, "unexpected chunked id %v", id)
		return 0
	}
	var iter int
	writer.randFn = func() float64 {
		iter++
		if iter%2 == 0 {
			return 0.1
		}
		return 0.9
	}

	inputs := []aggregated.ChunkedMetricWithStoragePolicy{
		testChunkedMetricWithStoragePolicy,
		testChunkedMetricWithStoragePolicy2,
		testChunkedMetricWithStoragePolicy2,
		testChunkedMetricWithStoragePolicy,
	}
	for _, input := range inputs {
		require.NoError(t, writer.Write(input))
	}

	encodedAtNanos := now.UnixNano()
	expectedData := map[uint32][]decodeData{
		1: []decodeData{
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy,
				encodedAtNanos:          0,
			},
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy,
				encodedAtNanos:          encodedAtNanos,
			},
		},
		2: []decodeData{
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy2,
				encodedAtNanos:          encodedAtNanos,
			},
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy2,
				encodedAtNanos:          0,
			},
		},
	}
	for s, expected := range expectedData {
		actual := actualData[s]
		require.Equal(t, expected, actual)
	}
	require.NoError(t, writer.Close())

}

func testProtobufWriter(t *testing.T, ctrl *gomock.Controller, opts Options) *protobufWriter {
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().NumShards().Return(uint32(1024))
	writer, err := NewProtobufWriter(p, opts)
	require.NoError(t, err)
	return writer.(*protobufWriter)
}
