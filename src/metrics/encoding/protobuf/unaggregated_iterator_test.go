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
	"io"
	"strings"
	"testing"

	"github.com/m3db/m3metrics/metric/unaggregated"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestUnaggregatedIteratorDecodeCounterWithMetadatas(t *testing.T) {
	inputs := []unaggregated.CounterWithMetadatas{
		{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas1,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas1,
		},
		{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas2,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(MessageUnion{
			Type:                 CounterWithMetadatasType,
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
		require.Equal(t, CounterWithMetadatasType, res.Type)
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
			StagedMetadatas: testMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testMetadatas2,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(MessageUnion{
			Type: BatchTimerWithMetadatasType,
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
		require.Equal(t, BatchTimerWithMetadatasType, res.Type)
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
			StagedMetadatas: testMetadatas1,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas1,
		},
		{
			Gauge:           testGauge1,
			StagedMetadatas: testMetadatas2,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(MessageUnion{
			Type:               GaugeWithMetadatasType,
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
		require.Equal(t, GaugeWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.GaugeWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeStress(t *testing.T) {
	inputs := []interface{}{
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas1,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge1,
			StagedMetadatas: testMetadatas2,
		},
		unaggregated.CounterWithMetadatas{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas2,
		},
		unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas2,
		},
		unaggregated.GaugeWithMetadatas{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas2,
		},
	}

	numIter := 1000
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	for iter := 0; iter < numIter; iter++ {
		for _, input := range inputs {
			var msg MessageUnion
			switch input := input.(type) {
			case unaggregated.CounterWithMetadatas:
				msg = MessageUnion{
					Type:                 CounterWithMetadatasType,
					CounterWithMetadatas: input,
				}
			case unaggregated.BatchTimerWithMetadatas:
				msg = MessageUnion{
					Type: BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: input,
				}
			case unaggregated.GaugeWithMetadatas:
				msg = MessageUnion{
					Type:               GaugeWithMetadatasType,
					GaugeWithMetadatas: input,
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
			require.Equal(t, CounterWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.CounterWithMetadatas, testCmpOpts...))
		case unaggregated.BatchTimerWithMetadatas:
			require.Equal(t, BatchTimerWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.BatchTimerWithMetadatas, testCmpOpts...))
		case unaggregated.GaugeWithMetadatas:
			require.Equal(t, GaugeWithMetadatasType, res.Type)
			require.True(t, cmp.Equal(expectedRes, res.GaugeWithMetadatas, testCmpOpts...))
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
		StagedMetadatas: testMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(MessageUnion{
		Type:               GaugeWithMetadatasType,
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
		StagedMetadatas: testMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(MessageUnion{
		Type:               GaugeWithMetadatasType,
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
		StagedMetadatas: testMetadatas1,
	}
	enc := NewUnaggregatedEncoder(NewUnaggregatedOptions())
	require.NoError(t, enc.EncodeMessage(MessageUnion{
		Type:               GaugeWithMetadatasType,
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
