// Copyright (c) 2017 Uber Technologies, Inc.
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
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testChunkedID = id.ChunkedID{
		Prefix: []byte("testPrefix."),
		Data:   []byte("testData"),
		Suffix: []byte(".testSuffix"),
	}
	testChunkedID2 = id.ChunkedID{
		Prefix: []byte("testPrefix2."),
		Data:   []byte("testData2"),
		Suffix: []byte(".testSuffix2"),
	}
	testChunkedMetricWithStoragePolicy = aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: testChunkedID,
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
)

func TestNewShardedWriterSharderError(t *testing.T) {
	sharderID := sharding.SharderID{}
	_, err := NewShardedWriter(sharderID, nil, NewOptions())
	require.Error(t, err)
}

func TestShardedWriterWriteClosed(t *testing.T) {
	writer := testShardedWriter(t, NewOptions())
	writer.closed = true
	require.Equal(t, errWriterClosed, writer.Write(testChunkedMetricWithStoragePolicy))
}

func TestShardedWriterWriteNoFlush(t *testing.T) {
	opts := NewOptions().SetMaxBufferSize(math.MaxInt64)
	writer := testShardedWriter(t, opts)
	writer.shardFn = func(chunkedID id.ChunkedID) uint32 {
		if isSameChunkedID(chunkedID, testChunkedID) {
			return 1
		}
		if isSameChunkedID(chunkedID, testChunkedID2) {
			return 2
		}
		require.Fail(t, "unexpected chunked id %v", chunkedID)
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

	expectedData := []struct {
		shard   uint32
		written []aggregated.ChunkedMetricWithStoragePolicy
	}{
		{
			shard: 1,
			written: []aggregated.ChunkedMetricWithStoragePolicy{
				testChunkedMetricWithStoragePolicy,
				testChunkedMetricWithStoragePolicy,
			},
		},
		{
			shard: 2,
			written: []aggregated.ChunkedMetricWithStoragePolicy{
				testChunkedMetricWithStoragePolicy2,
				testChunkedMetricWithStoragePolicy2,
			},
		},
	}
	for shard, encoder := range writer.encodersByShard {
		var (
			expectedWritten []aggregated.ChunkedMetricWithStoragePolicy
			found           bool
		)
		for _, expected := range expectedData {
			if expected.shard == uint32(shard) {
				expectedWritten = expected.written
				found = true
				break
			}
		}
		if found {
			actual := []*common.RefCountedBuffer{common.NewRefCountedBuffer(encoder.Encoder())}
			validateWritten(t, expectedWritten, actual)
		} else {
			require.Nil(t, encoder)
		}
	}
}

func TestShardedWriterWriteWithFlush(t *testing.T) {
	flushed := make(map[uint32][]*common.RefCountedBuffer)
	router := &mockRouter{
		routeFn: func(shard uint32, buf *common.RefCountedBuffer) error {
			flushed[shard] = append(flushed[shard], buf)
			return nil
		},
	}
	opts := NewOptions().SetMaxBufferSize(0)
	writer := testShardedWriter(t, opts)
	writer.Router = router
	writer.shardFn = func(chunkedID id.ChunkedID) uint32 {
		if isSameChunkedID(chunkedID, testChunkedID) {
			return 1
		}
		if isSameChunkedID(chunkedID, testChunkedID2) {
			return 2
		}
		require.Fail(t, "unexpected chunked id %v", chunkedID)
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

	expectedData := []struct {
		shard   uint32
		written []aggregated.ChunkedMetricWithStoragePolicy
	}{
		{
			shard: 1,
			written: []aggregated.ChunkedMetricWithStoragePolicy{
				testChunkedMetricWithStoragePolicy,
				testChunkedMetricWithStoragePolicy,
			},
		},
		{
			shard: 2,
			written: []aggregated.ChunkedMetricWithStoragePolicy{
				testChunkedMetricWithStoragePolicy2,
				testChunkedMetricWithStoragePolicy2,
			},
		},
	}
	require.Equal(t, len(expectedData), len(flushed))
	for _, expected := range expectedData {
		actual, exists := flushed[expected.shard]
		require.True(t, exists)
		validateWritten(t, expected.written, actual)
	}
}

func TestShardedWriterFlushClosed(t *testing.T) {
	writer := testShardedWriter(t, NewOptions())
	writer.closed = true
	require.Equal(t, errWriterClosed, writer.Flush())
}

func TestShardedWriterFlush(t *testing.T) {
	flushed := make(map[uint32][]*common.RefCountedBuffer)
	router := &mockRouter{
		routeFn: func(shard uint32, buf *common.RefCountedBuffer) error {
			if shard == 1 {
				return errors.New("error routing data")
			}
			flushed[shard] = append(flushed[shard], buf)
			return nil
		},
	}
	writer := testShardedWriter(t, NewOptions())
	writer.Router = router

	// Shard 0 has an empty buffer.
	writer.encodersByShard[0] = msgpack.NewAggregatedEncoder(msgpack.NewBufferedEncoder())

	// Shard 1 has a buffer with bad data.
	buf := []byte{1, 2, 3, 4}
	bufferedEncoder := msgpack.NewBufferedEncoder()
	_, err := bufferedEncoder.Buffer().Write(buf)
	require.NoError(t, err)
	writer.encodersByShard[1] = msgpack.NewAggregatedEncoder(bufferedEncoder)

	// Shard 2 has a buffer with good data.
	bufferedEncoder = msgpack.NewBufferedEncoder()
	writer.encodersByShard[2] = msgpack.NewAggregatedEncoder(bufferedEncoder)
	inputs := []aggregated.ChunkedMetricWithStoragePolicy{
		testChunkedMetricWithStoragePolicy,
		testChunkedMetricWithStoragePolicy2,
	}
	for _, input := range inputs {
		require.NoError(t, writer.encodersByShard[2].EncodeChunkedMetricWithStoragePolicy(input))
	}

	require.Error(t, writer.Flush())
	require.Equal(t, 1, len(flushed))
	actual, exists := flushed[2]
	require.True(t, exists)
	validateWritten(t, inputs, actual)
}

func TestShardedWriterCloseAlreadyClosed(t *testing.T) {
	writer := testShardedWriter(t, NewOptions())
	writer.closed = true
	require.Equal(t, errWriterClosed, writer.Close())
}

func TestShardedWriterClose(t *testing.T) {
	writer := testShardedWriter(t, NewOptions())
	require.NoError(t, writer.Close())
	require.True(t, writer.closed)
}

func isSameChunkedID(id1, id2 id.ChunkedID) bool {
	return bytes.Equal(id1.Prefix, id2.Prefix) &&
		bytes.Equal(id1.Data, id2.Data) &&
		bytes.Equal(id1.Suffix, id2.Suffix)
}

func validateWritten(
	t *testing.T,
	expected []aggregated.ChunkedMetricWithStoragePolicy,
	actual []*common.RefCountedBuffer,
) {
	var decoded []aggregated.MetricWithStoragePolicy
	it := msgpack.NewAggregatedIterator(nil, nil)
	for _, b := range actual {
		it.Reset(b.Buffer().Buffer())
		for it.Next() {
			rm, sp := it.Value()
			m, err := rm.Metric()
			require.NoError(t, err)
			decoded = append(decoded, aggregated.MetricWithStoragePolicy{
				Metric:        m,
				StoragePolicy: sp,
			})
		}
		b.DecRef()
		require.Equal(t, io.EOF, it.Err())
	}

	require.Equal(t, len(expected), len(decoded))
	for i := 0; i < len(decoded); i++ {
		chunkedID := expected[i].ChunkedID
		numBytes := len(chunkedID.Prefix) + len(chunkedID.Data) + len(chunkedID.Suffix)
		expectedID := make([]byte, numBytes)
		n := copy(expectedID, chunkedID.Prefix)
		n += copy(expectedID[n:], chunkedID.Data)
		copy(expectedID[n:], chunkedID.Suffix)
		require.Equal(t, expectedID, []byte(decoded[i].ID))
		require.Equal(t, expected[i].TimeNanos, decoded[i].TimeNanos)
		require.Equal(t, expected[i].Value, decoded[i].Value)
		require.Equal(t, expected[i].StoragePolicy, decoded[i].StoragePolicy)
	}
}

func testShardedWriter(t *testing.T, opts Options) *shardedWriter {
	sharderID := sharding.NewSharderID(sharding.DefaultHash, 1024)
	writer, err := NewShardedWriter(sharderID, nil, opts)
	require.NoError(t, err)
	return writer.(*shardedWriter)
}

type routeFn func(shard uint32, buf *common.RefCountedBuffer) error

type mockRouter struct {
	routeFn routeFn
}

func (r *mockRouter) Route(shard uint32, buf *common.RefCountedBuffer) error {
	return r.routeFn(shard, buf)
}

func (r *mockRouter) Close() {}
