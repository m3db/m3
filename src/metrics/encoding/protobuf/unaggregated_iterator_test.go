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

package protobuf

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestUnaggregatedIteratorDecodeCounterWithMetadatas(t *testing.T) {
	inputs := []unaggregated.CounterWithMetadatas{
		{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                 encoding.CounterWithMetadatasType,
			CounterWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.CounterWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.CounterWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithMetadatas(t *testing.T) {
	inputs := []unaggregated.BatchTimerWithMetadatas{
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                    encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.BatchTimerWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.BatchTimerWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeGaugeWithMetadatas(t *testing.T) {
	inputs := []unaggregated.GaugeWithMetadatas{
		{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas1,
		},
		{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas2,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:               encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.GaugeWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.GaugeWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeForwardedMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.ForwardedMetricWithMetadata{
		{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata1,
		},
		{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata1,
		},
		{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata2,
		},
		{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                        encoding.ForwardedMetricWithMetadataType,
			ForwardedMetricWithMetadata: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.ForwardedMetricWithMetadataType, res.Type)
		require.Equal(t, inputs[i], res.ForwardedMetricWithMetadata)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}
func TestUnaggregatedIteratorDecodePassthroughMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.PassthroughMetricWithMetadata{
		{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata1,
		},
		{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata1,
		},
		{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata2,
		},
		{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                          encoding.PassthroughMetricWithMetadataType,
			PassthroughMetricWithMetadata: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.PassthroughMetricWithMetadataType, res.Type)
		require.Equal(t, inputs[i], res.PassthroughMetricWithMetadata)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeTimedMetricWithMetadata(t *testing.T) {
	inputs := []aggregated.TimedMetricWithMetadata{
		{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata1,
		},
		{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata1,
		},
		{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata2,
		},
		{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                    encoding.TimedMetricWithMetadataType,
			TimedMetricWithMetadata: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.TimedMetricWithMetadataType, res.Type)
		require.Equal(t, inputs[i], res.TimedMetricWithMetadata)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeStress(t *testing.T) {
	inputs := []interface{}{
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas1,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata1,
		},
		aggregated.TimedMetricWithMetadata{
			Metric:        testTimedMetric1,
			TimedMetadata: testTimedMetadata1,
		},
		aggregated.PassthroughMetricWithMetadata{
			Metric:        testPassthroughMetric1,
			StoragePolicy: testPassthroughMetadata1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas1,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testStagedMetadatas2,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric1,
			ForwardMetadata: testForwardMetadata2,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testStagedMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testStagedMetadatas2,
		},
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwardedMetric2,
			ForwardMetadata: testForwardMetadata2,
		},
		aggregated.TimedMetricWithMetadata{
			Metric:        testTimedMetric2,
			TimedMetadata: testTimedMetadata2,
		},
		aggregated.PassthroughMetricWithMetadata{
			Metric:        testPassthroughMetric2,
			StoragePolicy: testPassthroughMetadata2,
		},
	}

	numIter := 1000
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for iter := 0; iter < numIter; iter++ {
		for _, input := range inputs {
			var msg encoding.UnaggregatedMessageUnion
			switch input := input.(type) {
			case unaggregated.CounterWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                 encoding.CounterWithMetadatasType,
					CounterWithMetadatas: input,
				}
			case unaggregated.BatchTimerWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                    encoding.BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: input,
				}
			case unaggregated.GaugeWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:               encoding.GaugeWithMetadatasType,
					GaugeWithMetadatas: input,
				}
			case aggregated.ForwardedMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                        encoding.ForwardedMetricWithMetadataType,
					ForwardedMetricWithMetadata: input,
				}
			case aggregated.TimedMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                    encoding.TimedMetricWithMetadataType,
					TimedMetricWithMetadata: input,
				}
			case aggregated.PassthroughMetricWithMetadata:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                          encoding.PassthroughMetricWithMetadataType,
					PassthroughMetricWithMetadata: input,
				}
			default:
				require.Fail(t, "unrecognized type %T", input)
			}
			require.NoError(t, enc.EncodeMessage(msg))
		}
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	defer it.Close()
	for it.Next() {
		res := it.Current()
		j := i % len(inputs)
		switch expectedRes := inputs[j].(type) {
		case unaggregated.CounterWithMetadatas:
			require.Equal(t, encoding.CounterWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.CounterWithMetadatas, testCmpOpts...))
		case unaggregated.BatchTimerWithMetadatas:
			require.Equal(t, encoding.BatchTimerWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.BatchTimerWithMetadatas, testCmpOpts...))
		case unaggregated.GaugeWithMetadatas:
			require.Equal(t, encoding.GaugeWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.GaugeWithMetadatas, testCmpOpts...))
		case aggregated.ForwardedMetricWithMetadata:
			require.Equal(t, encoding.ForwardedMetricWithMetadataType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.ForwardedMetricWithMetadata, testCmpOpts...))
		case aggregated.TimedMetricWithMetadata:
			require.Equal(t, encoding.TimedMetricWithMetadataType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.TimedMetricWithMetadata, testCmpOpts...))
		case aggregated.PassthroughMetricWithMetadata:
			require.Equal(t, encoding.PassthroughMetricWithMetadataType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.PassthroughMetricWithMetadata, testCmpOpts...))
		default:
			require.Fail(t, "unknown input type: %T", inputs[j])
		}
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs)*numIter, i)
}

func TestUnaggregatedIteratorMessageTooLarge(t *testing.T) {
	input := unaggregated.GaugeWithMetadatas{
		Gauge:           testGauge1,
		StagedMetadatas: testStagedMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
		Type:               encoding.GaugeWithMetadatasType,
		GaugeWithMetadatas: input,
	}))
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions().SetMaxMessageSize(1))
	defer it.Close()
	for it.Next() {
		i++
	}
	err := it.Err()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "larger than supported max message size"))
	require.Equal(t, 0, i)
}

func TestUnaggregatedIteratorNextOnError(t *testing.T) {
	input := unaggregated.GaugeWithMetadatas{
		Gauge:           testGauge1,
		StagedMetadatas: testStagedMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
		Type:               encoding.GaugeWithMetadatasType,
		GaugeWithMetadatas: input,
	}))
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	stream := bytes.NewReader(dataBuf.Bytes())
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions().SetMaxMessageSize(1))
	require.False(t, it.Next())
	require.False(t, it.Next())
}

func TestUnaggregatedIteratorNextOnClose(t *testing.T) {
	input := unaggregated.GaugeWithMetadatas{
		Gauge:           testGauge1,
		StagedMetadatas: testStagedMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
		Type:               encoding.GaugeWithMetadatasType,
		GaugeWithMetadatas: input,
	}))
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	stream := bytes.NewReader(dataBuf.Bytes())
	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	iterator := it.(*unaggregatedIterator)
	require.False(t, iterator.closed)
	require.NotNil(t, iterator.buf)
	require.Nil(t, it.Err())

	// Verify that closing the iterator cleans up the state.
	it.Close()
	require.False(t, it.Next())
	require.False(t, it.Next())
	require.True(t, iterator.closed)
	require.Nil(t, iterator.bytesPool)
	require.Nil(t, iterator.buf)
	require.Nil(t, it.Err())

	// Verify that closing a second time is a no op.
	it.Close()
}

func TestUnaggregatedIteratorNextOnInvalid(t *testing.T) {
	buf := make([]byte, 32)
	binary.PutVarint(buf, 0)
	stream := bytes.NewReader(buf)

	it := NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	require.False(t, it.Next())
	require.False(t, it.Next())

	buf = make([]byte, 32)
	binary.PutVarint(buf, -1234)
	stream = bytes.NewReader(buf)
	it = NewUnaggregatedIterator(stream, NewUnaggregatedOptions())
	require.False(t, it.Next())
	require.False(t, it.Next())
}
