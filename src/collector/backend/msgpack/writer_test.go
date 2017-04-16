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
	require.Equal(t, errInstanceWriterClosed, w.Write(0, testCounter, testVersionedPolicies))
}

func TestWriterWriteEncodeError(t *testing.T) {
	errTestEncodeMetric := errors.New("error encoding metrics")
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				counterFn: func(cp unaggregated.CounterWithPolicies) error {
					return errTestEncodeMetric
				},
			},
		}
	}
	require.Equal(t, errTestEncodeMetric, w.Write(0, testCounter, testVersionedPolicies))
}

func TestWriteEncoderExists(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	tmpEncoder := msgpack.NewBufferedEncoder()
	w.encodersByShard[0] = &lockedEncoder{
		UnaggregatedEncoder: &mockEncoder{
			encoder: tmpEncoder,
			counterFn: func(cp unaggregated.CounterWithPolicies) error {
				tmpEncoder.Buffer().Write([]byte{0x4, 0x5, 0x6, 0x7})
				return nil
			},
		},
	}
	require.NoError(t, w.Write(0, testCounter, testVersionedPolicies))
	require.Equal(t, []byte{0x4, 0x5, 0x6, 0x7}, tmpEncoder.Buffer().Bytes())
	require.Equal(t, 1, len(w.encodersByShard))
}

func TestWriterWriteEncodeSuccessNoFlushing(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	require.NoError(t, w.Write(0, testCounter, testVersionedPolicies))
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
				counterFn: func(cp unaggregated.CounterWithPolicies) error {
					tmpEncoder.Buffer().Write([]byte{0x4, 0x5, 0x6, 0x7})
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testCounter, testVersionedPolicies))
	require.Equal(t, []byte{0x4, 0x5, 0x6, 0x7}, w.encodersByShard[0].Encoder().Bytes())
	require.Equal(t, []byte{0x1, 0x2, 0x3}, bufRes.Bytes())
	require.Equal(t, 1, len(w.encodersByShard))
}

func TestWriterWriteCounterWithPolicies(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.Counter
		vpRes policy.VersionedPolicies
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				counterFn: func(cp unaggregated.CounterWithPolicies) error {
					muRes = cp.Counter
					vpRes = cp.VersionedPolicies
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testCounter, testVersionedPolicies))
	require.Equal(t, testCounter.Counter(), muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
}

func TestWriterWriteBatchTimerWithPolicies(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.BatchTimer
		vpRes policy.VersionedPolicies
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				batchTimerFn: func(btp unaggregated.BatchTimerWithPolicies) error {
					muRes = btp.BatchTimer
					vpRes = btp.VersionedPolicies
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testBatchTimer, testVersionedPolicies))
	require.Equal(t, testBatchTimer.BatchTimer(), muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
}

func TestWriterWriteGaugeWithPolicies(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testServerOptions()).(*writer)
	w.closed = false
	var (
		muRes unaggregated.Gauge
		vpRes policy.VersionedPolicies
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				gaugeFn: func(gp unaggregated.GaugeWithPolicies) error {
					muRes = gp.Gauge
					vpRes = gp.VersionedPolicies
					return nil
				},
			},
		}
	}
	require.NoError(t, w.Write(0, testGauge, testVersionedPolicies))
	require.Equal(t, testGauge.Gauge(), muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
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

type encodeCounterWithPoliciesFn func(cp unaggregated.CounterWithPolicies) error
type encodeBatchTimerWithPoliciesFn func(cp unaggregated.BatchTimerWithPolicies) error
type encodeGaugeWithPoliciesFn func(cp unaggregated.GaugeWithPolicies) error

type mockEncoder struct {
	encoder      msgpack.BufferedEncoder
	counterFn    encodeCounterWithPoliciesFn
	batchTimerFn encodeBatchTimerWithPoliciesFn
	gaugeFn      encodeGaugeWithPoliciesFn
}

func (me *mockEncoder) EncodeCounterWithPolicies(cp unaggregated.CounterWithPolicies) error {
	return me.counterFn(cp)
}

func (me *mockEncoder) EncodeBatchTimerWithPolicies(btp unaggregated.BatchTimerWithPolicies) error {
	return me.batchTimerFn(btp)
}

func (me *mockEncoder) EncodeGaugeWithPolicies(gp unaggregated.GaugeWithPolicies) error {
	return me.gaugeFn(gp)
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
