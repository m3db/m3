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
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testChunkedID = id.ChunkedID{
		Prefix: []byte("testPrefix."),
		Data:   []byte("testData"),
		Suffix: []byte(".testSuffix"),
	}
	testRawID      = []byte("testPrefix.testData.testSuffix")
	testChunkedID2 = id.ChunkedID{
		Prefix: []byte("testPrefix2."),
		Data:   []byte("testData2"),
		Suffix: []byte(".testSuffix2"),
	}
	testRawID2                         = []byte("testPrefix2.testData2.testSuffix2")
	testChunkedMetricWithStoragePolicy = aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: testChunkedID,
			TimeNanos: 123456,
			Value:     3.14,
		},
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
	}
	testMetricWithStoragePolicy = aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        testRawID,
			TimeNanos: 123456,
			Value:     3.14,
		},
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
	}
	testChunkedMetricWithStoragePolicy2 = aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: testChunkedID2,
			TimeNanos: 1000,
			Value:     987,
		},
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}
	testMetricWithStoragePolicy2 = aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			ID:        testRawID2,
			TimeNanos: 1000,
			Value:     987,
		},
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
	}
)

func TestStoragePolicyFilter(t *testing.T) {
	sp1 := policy.NewStoragePolicy(time.Minute, xtime.Second, time.Hour)
	sp2 := policy.NewStoragePolicy(time.Second, xtime.Second, time.Minute)

	m2 := producer.NewMockMessage(nil)

	f := NewStoragePolicyFilter([]policy.StoragePolicy{sp2})

	require.True(t, f(m2))
	require.False(t, f(newMessage(0, sp1, protobuf.Buffer{})))
	require.True(t, f(newMessage(0, sp2, protobuf.Buffer{})))
}

func TestProtobufWriterWriteClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writer := testProtobufWriter(t, ctrl, NewOptions())
	writer.closed = true
	require.Equal(t, errWriterClosed, writer.Write(testChunkedMetricWithStoragePolicy))
}

func TestProtobufWriterWrite(t *testing.T) {
	opts := NewOptions()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	writer := testProtobufWriter(t, ctrl, opts)

	actualData := make(map[uint32][]decodeData)
	writer.p.(*producer.MockProducer).EXPECT().Produce(gomock.Any()).Do(func(m producer.Message) error {
		d := protobuf.NewAggregatedDecoder(nil)
		require.NoError(t, d.Decode(m.Bytes()))
		s := m.Shard()
		sp := d.StoragePolicy()
		actualData[s] = append(actualData[s], decodeData{
			MetricWithStoragePolicy: aggregated.MetricWithStoragePolicy{
				Metric: aggregated.Metric{
					ID:        d.ID(),
					TimeNanos: d.TimeNanos(),
					Value:     d.Value(),
				},
				StoragePolicy: sp,
			},
		})
		return nil
	}).AnyTimes()
	writer.shardFn = func(id []byte, s uint32) uint32 {
		if bytes.Equal(id, testRawID) {
			return 1
		}
		if bytes.Equal(id, testRawID2) {
			return 2
		}
		require.Fail(t, "unexpected chunked id %v", id)
		return 0
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
	expectedData := map[uint32][]decodeData{
		1: {
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy,
			},
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy,
			},
		},
		2: {
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy2,
			},
			{
				MetricWithStoragePolicy: testMetricWithStoragePolicy2,
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
	return NewProtobufWriter(p, sharding.Murmur32Hash.MustShardFn(), opts).(*protobufWriter)
}

type decodeData struct {
	aggregated.MetricWithStoragePolicy
}
