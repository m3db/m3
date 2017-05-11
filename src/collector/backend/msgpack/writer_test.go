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

package msgpack

import (
	"errors"
	"testing"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"

	"github.com/stretchr/testify/require"
)

func TestWriterWriteClosed(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = true
	require.Equal(t, errInstanceWriterClosed, w.Write(0, testCounter, testPoliciesList))
}

func TestWriterWriteEncodeError(t *testing.T) {
	errTestEncodeMetric := errors.New("error encoding metrics")
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				counterWithPoliciesListFn: func(cp unaggregated.CounterWithPoliciesList) error {
					return errTestEncodeMetric
				},
			},
		}
	}
	require.Equal(t, errTestEncodeMetric, w.Write(0, testCounter, testPoliciesList))
}

func TestWriteEncoderExists(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	tmpEncoder := msgpack.NewBufferedEncoder()
	w.encodersByShard[0] = &lockedEncoder{
		UnaggregatedEncoder: &mockEncoder{
			encoder: tmpEncoder,
			counterWithPoliciesListFn: func(cp unaggregated.CounterWithPoliciesList) error {
				tmpEncoder.Buffer().Write([]byte{0x4, 0x5, 0x6, 0x7})
				return nil
			},
		},
	}
	require.NoError(t, w.Write(0, testCounter, testPoliciesList))
	require.Equal(t, []byte{0x4, 0x5, 0x6, 0x7}, tmpEncoder.Buffer().Bytes())
	require.Equal(t, 1, len(w.encodersByShard))
}

func TestWriterWriteEncodeSuccessNoFlushing(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	require.NoError(t, w.Write(0, testCounter, testPoliciesList))
	require.Equal(t, 1, len(w.encodersByShard))
	_, exists := w.encodersByShard[0]
	require.True(t, exists)
}

func TestWriterWriteEncodeSuccessWithFlushing(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions().SetFlushSize(3)).(*writer)
	w.closed = false
	var bufRes msgpack.Buffer
	w.queue = &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error {
			bufRes = buf
			return nil
		},
	}

	tmpEncoder := msgpack.NewBufferedEncoder()
	tmpEncoder.Buffer().Write([]byte{0x1, 0x2, 0x3})
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: tmpEncoder,
				counterWithPoliciesListFn: func(cp unaggregated.CounterWithPoliciesList) error {
					tmpEncoder.Buffer().Write([]byte{0x4, 0x5, 0x6, 0x7})
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testCounter, testPoliciesList))
	require.Equal(t, []byte{0x4, 0x5, 0x6, 0x7}, w.encodersByShard[0].Encoder().Bytes())
	require.Equal(t, []byte{0x1, 0x2, 0x3}, bufRes.Bytes())
	require.Equal(t, 1, len(w.encodersByShard))
}

func TestWriterWriteCounterWithPoliciesList(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.Counter
		plRes policy.PoliciesList
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				counterWithPoliciesListFn: func(cp unaggregated.CounterWithPoliciesList) error {
					muRes = cp.Counter
					plRes = cp.PoliciesList
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testCounter, testPoliciesList))
	require.Equal(t, testCounter.Counter(), muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestWriterWriteBatchTimerWithPoliciesList(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.BatchTimer
		plRes policy.PoliciesList
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				batchTimerWithPoliciesListFn: func(btp unaggregated.BatchTimerWithPoliciesList) error {
					muRes = btp.BatchTimer
					plRes = btp.PoliciesList
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testBatchTimer, testPoliciesList))
	require.Equal(t, testBatchTimer.BatchTimer(), muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestWriterWriteGaugeWithPoliciesList(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.Gauge
		plRes policy.PoliciesList
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				gaugeWithPoliciesListFn: func(gp unaggregated.GaugeWithPoliciesList) error {
					muRes = gp.Gauge
					plRes = gp.PoliciesList
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testGauge, testPoliciesList))
	require.Equal(t, testGauge.Gauge(), muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestWriterFlushClosed(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = true
	require.Equal(t, errInstanceWriterClosed, w.Flush())
}

func TestWriterFlushPartialError(t *testing.T) {
	opts := testServerOptions()
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false
	var (
		numBytes int
		count    int
	)
	w.queue = &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error {
			numBytes += len(buf.Bytes())
			count++
			if count == 1 {
				return errors.New("flush error")
			}
			return nil
		},
	}

	for i := 0; i < 3; i++ {
		w.encodersByShard[uint32(i)] = w.newLockedEncoderFn(opts.BufferedEncoderPool())
	}
	w.encodersByShard[1].Encoder().Buffer().Write([]byte{0x1, 0x2})
	w.encodersByShard[2].Encoder().Buffer().Write([]byte{0x3, 0x4, 0x5, 0x6})
	require.Error(t, w.Flush())
	require.Equal(t, 6, numBytes)
}

func TestWriterCloseAlreadyClosed(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = true
	require.Equal(t, errInstanceWriterClosed, w.Close())
}

func TestWriterCloseSuccess(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	require.NoError(t, w.Close())
}

func TestRefCountedWriter(t *testing.T) {
	opts := testServerOptions()
	w := newRefCountedWriter(testPlacementInstance, opts)
	w.IncRef()

	require.False(t, w.instanceWriter.(*writer).closed)
	w.DecRef()
	require.True(t, w.instanceWriter.(*writer).closed)
}

type encodeCounterWithPoliciesListFn func(cp unaggregated.CounterWithPoliciesList) error
type encodeBatchTimerWithPoliciesListFn func(cp unaggregated.BatchTimerWithPoliciesList) error
type encodeGaugeWithPoliciesListFn func(cp unaggregated.GaugeWithPoliciesList) error

type mockEncoder struct {
	encoder                      msgpack.BufferedEncoder
	counterWithPoliciesListFn    encodeCounterWithPoliciesListFn
	batchTimerWithPoliciesListFn encodeBatchTimerWithPoliciesListFn
	gaugeWithPoliciesListFn      encodeGaugeWithPoliciesListFn
}

func (me *mockEncoder) EncodeCounter(c unaggregated.Counter) error       { return nil }
func (me *mockEncoder) EncodeBatchTimer(c unaggregated.BatchTimer) error { return nil }
func (me *mockEncoder) EncodeGauge(c unaggregated.Gauge) error           { return nil }

func (me *mockEncoder) EncodeCounterWithPoliciesList(cp unaggregated.CounterWithPoliciesList) error {
	return me.counterWithPoliciesListFn(cp)
}

func (me *mockEncoder) EncodeBatchTimerWithPoliciesList(btp unaggregated.BatchTimerWithPoliciesList) error {
	return me.batchTimerWithPoliciesListFn(btp)
}

func (me *mockEncoder) EncodeGaugeWithPoliciesList(gp unaggregated.GaugeWithPoliciesList) error {
	return me.gaugeWithPoliciesListFn(gp)
}

func (me *mockEncoder) Encoder() msgpack.BufferedEncoder {
	return me.encoder
}

func (me *mockEncoder) Reset(encoder msgpack.BufferedEncoder) {
	me.encoder = encoder
}

type enqueueFn func(buf msgpack.Buffer) error

type mockInstanceQueue struct {
	enqueueFn enqueueFn
}

func (mq *mockInstanceQueue) Enqueue(buf msgpack.Buffer) error { return mq.enqueueFn(buf) }
func (mq *mockInstanceQueue) Close() error                     { return nil }
