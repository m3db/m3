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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
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

func TestWriterWriteBatchTimerWithPoliciesListNoBatchSizeLimit(t *testing.T) {
	numValues := 65536
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: timerValues,
	}

	opts := testServerOptions().SetMaxTimerBatchSize(0)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
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
	require.NoError(t, w.Write(0, testLargeBatchTimer, testPoliciesList))
	require.Equal(t, testLargeBatchTimer.BatchTimer(), muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestWriterWriteBatchTimerWithPoliciesListSmallBatchSize(t *testing.T) {
	opts := testServerOptions().SetMaxTimerBatchSize(140)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
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

func TestWriterWriteBatchTimerWithPoliciesListLargeBatchSize(t *testing.T) {
	numValues := 65536
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: timerValues,
	}

	opts := testServerOptions().SetMaxTimerBatchSize(140)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false
	var (
		idRes    []id.RawID
		valueRes [][]float64
		plRes    []policy.PoliciesList
	)
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				batchTimerWithPoliciesListFn: func(btp unaggregated.BatchTimerWithPoliciesList) error {
					idRes = append(idRes, btp.ID)
					valueRes = append(valueRes, btp.Values)
					plRes = append(plRes, btp.PoliciesList)
					return nil
				},
			},
		}
	}

	require.NoError(t, w.Write(0, testLargeBatchTimer, testPoliciesList))

	var (
		maxBatchSize         = w.maxTimerBatchSize
		expectedNumBatches   = int(math.Ceil(float64(numValues) / float64(maxBatchSize)))
		expectedIDs          []id.RawID
		expectedValues       [][]float64
		expectedPoliciesList []policy.PoliciesList
	)

	for i := 0; i < expectedNumBatches; i++ {
		start := i * maxBatchSize
		end := start + maxBatchSize
		if end > numValues {
			end = numValues
		}
		expectedValues = append(expectedValues, timerValues[start:end])
		expectedIDs = append(expectedIDs, id.RawID("foo"))
		expectedPoliciesList = append(expectedPoliciesList, testPoliciesList)
	}

	require.Equal(t, expectedIDs, idRes)
	require.Equal(t, expectedValues, valueRes)
	require.Equal(t, expectedPoliciesList, plRes)
}

func TestWriterWriteLargeBatchTimerUsesMultipleBuffers(t *testing.T) {
	numValues := 1400
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: timerValues,
	}

	testScope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(testScope)
	opts := testServerOptions().
		SetMaxTimerBatchSize(140).
		SetInstrumentOptions(iOpts)

	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false

	require.NoError(t, w.Write(0, testLargeBatchTimer, testPoliciesList))

	enqueuedCounter := testScope.Snapshot().Counters()["buffers+action=enqueued"]
	require.NotNil(t, enqueuedCounter)
	require.Equal(t, int64(5), enqueuedCounter.Value())
}

func TestWriterWriteBatchTimerWithPoliciesListWriteError(t *testing.T) {
	numValues := 7
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: timerValues,
	}

	var (
		errOnIter = 2
		numIters  int
		errWrite  = errors.New("write error")
	)

	opts := testServerOptions().SetMaxTimerBatchSize(3)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false
	w.newLockedEncoderFn = func(msgpack.BufferedEncoderPool) *lockedEncoder {
		return &lockedEncoder{
			UnaggregatedEncoder: &mockEncoder{
				encoder: msgpack.NewBufferedEncoder(),
				batchTimerWithPoliciesListFn: func(btp unaggregated.BatchTimerWithPoliciesList) error {
					numIters++
					if numIters == errOnIter {
						return errWrite
					}
					return nil
				},
			},
		}
	}

	require.Equal(t, errWrite, w.Write(0, testLargeBatchTimer, testPoliciesList))
	require.Equal(t, errOnIter, numIters)
}

func TestWriterWriteBatchTimerWithPoliciesListEnqueueError(t *testing.T) {
	errTestEnqueue := errors.New("test enqueue error")
	opts := testServerOptions().
		SetMaxTimerBatchSize(1).
		SetFlushSize(1)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false
	w.queue = &mockInstanceQueue{
		enqueueFn: func(msgpack.Buffer) error { return errTestEnqueue },
	}

	require.Equal(t, errTestEnqueue, w.Write(0, testBatchTimer, testPoliciesList))
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

func TestWriterConcurrentWriteStress(t *testing.T) {
	params := []struct {
		maxInputBatchSize int
		maxTimerBatchSize int
		flushSize         int
	}{
		// High likelihood of counter/gauge encoding triggering a flush in between
		// releasing and re-acquiring locks when encoding large timer batches.
		{
			maxInputBatchSize: 150,
			maxTimerBatchSize: 150,
			flushSize:         1000,
		},
		// Large timer batches.
		{
			maxInputBatchSize: 1000,
			maxTimerBatchSize: 140,
			flushSize:         1440,
		},
	}

	for _, param := range params {
		testWriterConcurrentWriteStress(
			t,
			param.maxInputBatchSize,
			param.maxTimerBatchSize,
			param.flushSize,
		)
	}
}

func testWriterConcurrentWriteStress(
	t *testing.T,
	maxInputBatchSize int,
	maxTimerBatchSize int,
	flushSize int,
) {
	var (
		numIter     = 10000
		shard       = uint32(0)
		counters    = make([]unaggregated.Counter, numIter)
		timers      = make([]unaggregated.BatchTimer, numIter)
		gauges      = make([]unaggregated.Gauge, numIter)
		resultsLock sync.Mutex
		results     [][]byte
	)

	// Construct metrics input.
	for i := 0; i < numIter; i++ {
		counters[i] = unaggregated.Counter{
			ID:    []byte(fmt.Sprintf("counter%d", i)),
			Value: int64(i),
		}
		gauges[i] = unaggregated.Gauge{
			ID:    []byte(fmt.Sprintf("gauge%d", i)),
			Value: float64(i),
		}
		batchSize := numIter - i
		if batchSize > maxInputBatchSize {
			batchSize = maxInputBatchSize
		}
		timerVals := make([]float64, batchSize)
		for j := i; j < i+batchSize; j++ {
			timerVals[j-i] = float64(j)
		}
		timers[i] = unaggregated.BatchTimer{
			ID:     []byte(fmt.Sprintf("timer%d", i)),
			Values: timerVals,
		}
	}

	opts := testServerOptions().
		SetMaxTimerBatchSize(maxTimerBatchSize).
		SetFlushSize(flushSize)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.closed = false
	w.queue = &mockInstanceQueue{
		enqueueFn: func(buf msgpack.Buffer) error {
			bytes := buf.Bytes()
			cloned := make([]byte, len(bytes))
			copy(cloned, bytes)
			resultsLock.Lock()
			results = append(results, cloned)
			resultsLock.Unlock()
			return nil
		},
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:       unaggregated.CounterType,
				ID:         counters[i].ID,
				CounterVal: counters[i].Value,
			}
			require.NoError(t, w.Write(shard, mu, testPoliciesList))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:          unaggregated.BatchTimerType,
				ID:            timers[i].ID,
				BatchTimerVal: timers[i].Values,
			}
			require.NoError(t, w.Write(shard, mu, testPoliciesList))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:     unaggregated.GaugeType,
				ID:       gauges[i].ID,
				GaugeVal: gauges[i].Value,
			}
			require.NoError(t, w.Write(shard, mu, testPoliciesList))
		}
	}()

	wg.Wait()
	w.Flush()

	var (
		resCounters = make([]unaggregated.Counter, 0, numIter)
		resTimers   = make([]unaggregated.BatchTimer, 0, numIter)
		resGauges   = make([]unaggregated.Gauge, 0, numIter)
	)
	for i := 0; i < len(results); i++ {
		buf := bytes.NewBuffer(results[i])
		iter := msgpack.NewUnaggregatedIterator(buf, msgpack.NewUnaggregatedIteratorOptions())
		for iter.Next() {
			policiesList := iter.PoliciesList()
			require.Equal(t, testPoliciesList, policiesList)
			metric := cloneMetric(iter.Metric())
			switch metric.Type {
			case unaggregated.CounterType:
				resCounters = append(resCounters, metric.Counter())
			case unaggregated.BatchTimerType:
				resTimers = append(resTimers, metric.BatchTimer())
			case unaggregated.GaugeType:
				resGauges = append(resGauges, metric.Gauge())
			default:
				require.Fail(t, "unrecognized metric type %v", metric.Type)
			}
		}
		require.Equal(t, io.EOF, iter.Err())
	}

	// Sort counters for comparison purposes.
	sort.Slice(counters, func(i, j int) bool {
		return bytes.Compare(counters[i].ID, counters[j].ID) < 0
	})
	sort.Slice(resCounters, func(i, j int) bool {
		return bytes.Compare(resCounters[i].ID, resCounters[j].ID) < 0
	})
	require.Equal(t, counters, resCounters)

	// Sort timers for comparison purposes.
	sort.Slice(timers, func(i, j int) bool {
		return bytes.Compare(timers[i].ID, timers[j].ID) < 0
	})
	sort.Slice(resTimers, func(i, j int) bool {
		return bytes.Compare(resTimers[i].ID, resTimers[j].ID) < 0
	})
	// Merge timers if necessary for comparison since they may be split into multiple batches.
	mergedResTimers := make([]unaggregated.BatchTimer, 0, numIter)
	curr := 0
	for i := 0; i < len(resTimers); i++ {
		if bytes.Equal(resTimers[curr].ID, resTimers[i].ID) {
			continue
		}
		var mergedValues []float64
		for j := curr; j < i; j++ {
			mergedValues = append(mergedValues, resTimers[j].Values...)
		}
		sort.Float64s(mergedValues)
		mergedResTimers = append(mergedResTimers, unaggregated.BatchTimer{
			ID:     resTimers[curr].ID,
			Values: mergedValues,
		})
		curr = i
	}
	if curr < len(resTimers) {
		var mergedValues []float64
		for j := curr; j < len(resTimers); j++ {
			mergedValues = append(mergedValues, resTimers[j].Values...)
		}
		sort.Float64s(mergedValues)
		mergedResTimers = append(mergedResTimers, unaggregated.BatchTimer{
			ID:     resTimers[curr].ID,
			Values: mergedValues,
		})
	}
	require.Equal(t, timers, mergedResTimers)

	// Sort gauges for comparison purposes.
	sort.Slice(gauges, func(i, j int) bool {
		return bytes.Compare(gauges[i].ID, gauges[j].ID) < 0
	})
	sort.Slice(resGauges, func(i, j int) bool {
		return bytes.Compare(resGauges[i].ID, resGauges[j].ID) < 0
	})
	require.Equal(t, gauges, resGauges)
}

func TestRefCountedWriter(t *testing.T) {
	opts := testServerOptions()
	w := newRefCountedWriter(testPlacementInstance, opts)
	w.IncRef()

	require.False(t, w.instanceWriter.(*writer).closed)
	w.DecRef()
	require.True(t, w.instanceWriter.(*writer).closed)
}

func cloneMetric(m unaggregated.MetricUnion) unaggregated.MetricUnion {
	mu := m
	clonedID := make(id.RawID, len(m.ID))
	copy(clonedID, m.ID)
	mu.ID = clonedID
	if m.Type == unaggregated.BatchTimerType {
		clonedTimerVal := make([]float64, len(m.BatchTimerVal))
		copy(clonedTimerVal, m.BatchTimerVal)
		mu.BatchTimerVal = clonedTimerVal
	}
	return mu
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
