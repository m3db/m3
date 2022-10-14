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

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestWriterWriteClosed(t *testing.T) {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.closed.Store(true)
	require.Equal(t, ErrInstanceWriterClosed, w.Write(0, payload))
}

func TestWriterWriteUntimedCounterEncodeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestEncodeMetric := errors.New("error encoding metrics")
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
		encoder.EXPECT().Len().Return(0)
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testCounter.Counter(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(errTestEncodeMetric)
		encoder.EXPECT().Truncate(0).Return(nil)
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.Equal(t, errTestEncodeMetric, w.Write(0, payload))
}

// TestWriterFlushInProgress tests that a writer does not
// have multiple flushes in progress at the same time. It
// can only ever have one flush active at any given point in
// time.
func TestWriterFlushInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockCh := make(chan bool)
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, 2*time.Second)
	defer cancelFn()

	slowMockConn := NewMockConn(ctrl)
	slowMockConn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (n int, err error) {
		// notify that the slow writer is about to block
		blockCh <- true
		<-ctx.Done()
		return len(b), nil
	})
	slowMockConn.EXPECT().SetWriteDeadline(gomock.Any())

	slowWriterDialerFn := func(c context.Context, network string, address string) (net.Conn, error) {
		return slowMockConn, nil
	}

	slowConnOpts := NewConnectionOptions().SetContextDialer(slowWriterDialerFn)
	opts := testOptions().SetConnectionOptions(slowConnOpts)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}

	require.Equal(t, nil, w.Write(0, payload))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Flush() //nolint:errcheck
	}()

	// wait for previous flush to block
	<-blockCh

	// now any newly initiated Flush should fail with err "in-progress"
	// Note that if we didn't wait on blockCh above then this following
	// Flush() can finish before the previous flush which defeats the
	// purpose of the test :-)
	require.EqualError(t, w.Flush(), ErrFlushInProgress.Error())

	// unblock the slow writer
	cancelFn()

	wg.Wait()
}

func TestWriterWriteUntimedCounterEncoderExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(0),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testCounter.Counter(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(4),
	)
	w.encodersByShard[0] = &lockedEncoder{UnaggregatedEncoder: encoder}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
	require.Equal(t, 1, len(w.encodersByShard))
}

func TestWriterWriteUntimedCounterEncoderDoesNotExist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testCounter.Counter(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
	)
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
}

func TestWriterEncoderSizeLimitItems(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions().SetMaxBatchSize(1).SetInstanceQueueSize(1)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}

	// Perform a write which should enqueue the buf
	require.NoError(t, w.Write(0, payload))
	sizeBefore := w.QueueSize()

	i := 0
	numWrites := 10
	for i < numWrites {
		require.NoError(t, w.Write(0, payload))
		i++
	}

	// All writes must respect the queue size of 1
	// meaning that after numWrites attempts,
	// the queueSize should still be 1 and the
	// enqueued len in the encoders must be 0
	// since all buffers would have been relinquished
	// on write since each write exceeded maxBatchSize of 1
	sizeAfter := w.QueueSize()

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, sizeBefore, sizeAfter)
	require.Equal(t, 0, w.encodersByShard[0].Len())
}

func TestWriterEncoderSizeLimitBytes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// The encoded length of the payload below is 155 bytes.
	// Set the MaxBatchSize to 200 so that it is relinquished
	// after we write the payload twice.
	// The queue is configured with a size limit of 300 bytes
	// Therefore it can hold one such relinquished buffer of total
	// size (155+155) 310. Any more attempts to write should
	// respect the size limit and the queue size should remain 310
	// Whereas the encoders should be fully drained for even number
	// of writes.
	opts := testOptions().SetMaxBatchSize(1).SetInstanceMaxQueueSizeBytes(300)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}

	i := 0
	numWrites := 10
	for i < numWrites {
		require.NoError(t, w.Write(0, payload))
		i++
	}

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, 310, w.QueueSizeBytes())
	require.Equal(t, 0, w.encodersByShard[0].Len())
}

//nolint:dupl
func TestWriterWriteUntimedCounterWithWriteZeroSizeBefore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		stream      = protobuf.NewBuffer([]byte{1, 2, 3, 4, 5, 6, 7}, nil)
		enqueuedBuf protobuf.Buffer
	)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(0),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testCounter.Counter(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
		encoder.EXPECT().Relinquish().Return(stream),
	)
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			enqueuedBuf = buf
			return nil
		})

	w := newInstanceWriter(testPlacementInstance, testOptions().SetMaxBatchSize(3)).(*writer)
	w.queue = queue
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7}, enqueuedBuf.Bytes())
}

//nolint:dupl
func TestWriterWriteUntimedCounterWithWritePositiveSizeBefore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		stream      = protobuf.NewBuffer([]byte{1, 2, 3, 4, 5, 6, 7}, nil)
		enqueuedBuf protobuf.Buffer
	)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testCounter.Counter(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
		encoder.EXPECT().Relinquish().Return(stream),
	)
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			enqueuedBuf = buf
			return nil
		})
	w := newInstanceWriter(testPlacementInstance, testOptions().SetMaxBatchSize(3)).(*writer)

	w.queue = queue
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7}, enqueuedBuf.Bytes())
}

func TestWriterWriteUntimedBatchTimerNoBatchSizeLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	numValues := 65536
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testLargeBatchTimer"),
		BatchTimerVal: timerValues,
	}
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      testLargeBatchTimer.BatchTimer(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
	)
	opts := testOptions().SetMaxTimerBatchSize(0)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testLargeBatchTimer,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
}

func TestWriterWriteUntimedBatchTimerSmallBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      testBatchTimer.BatchTimer(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
	)
	opts := testOptions().SetMaxTimerBatchSize(140)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testBatchTimer,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
}

func TestWriterWriteUntimedBatchTimerLargeBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	numValues := 65536
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testLargeBatchTimer"),
		BatchTimerVal: timerValues,
	}

	var (
		msgTypeRes         []encoding.UnaggregatedMessageType
		idRes              []id.RawID
		valueRes           [][]float64
		metadataRes        []metadata.StagedMetadatas
		maxBatchSize       = 140
		expectedNumBatches = int(math.Ceil(float64(numValues) / float64(maxBatchSize)))
	)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	encoder.EXPECT().Len().Return(7).MinTimes(2)
	encoder.EXPECT().
		EncodeMessage(gomock.Any()).
		DoAndReturn(func(msg encoding.UnaggregatedMessageUnion) error {
			msgTypeRes = append(msgTypeRes, msg.Type)
			idRes = append(idRes, msg.BatchTimerWithMetadatas.ID)
			valueRes = append(valueRes, msg.BatchTimerWithMetadatas.Values)
			metadataRes = append(metadataRes, msg.BatchTimerWithMetadatas.StagedMetadatas)
			return nil
		}).Times(expectedNumBatches)
	encoder.EXPECT().Relinquish()

	opts := testOptions().SetMaxTimerBatchSize(maxBatchSize)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testLargeBatchTimer,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
	require.NoError(t, w.Flush())

	var (
		expectedMsgTypes  []encoding.UnaggregatedMessageType
		expectedIDs       []id.RawID
		expectedValues    [][]float64
		expectedMetadatas []metadata.StagedMetadatas
	)
	for i := 0; i < expectedNumBatches; i++ {
		start := i * maxBatchSize
		end := start + maxBatchSize
		if end > numValues {
			end = numValues
		}
		expectedMsgTypes = append(expectedMsgTypes, encoding.BatchTimerWithMetadatasType)
		expectedValues = append(expectedValues, timerValues[start:end])
		expectedIDs = append(expectedIDs, id.RawID("testLargeBatchTimer"))
		expectedMetadatas = append(expectedMetadatas, testStagedMetadatas)
	}
	require.Equal(t, expectedMsgTypes, msgTypeRes)
	require.Equal(t, expectedIDs, idRes)
	require.Equal(t, expectedValues, valueRes)
	require.Equal(t, expectedMetadatas, metadataRes)
}

func TestWriterWriteUntimedLargeBatchTimerUsesMultipleBuffers(t *testing.T) {
	const (
		numValues  = 1400
		testIDName = "testLargeBatchTimer"
	)

	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}

	var (
		testLargeBatchTimer = unaggregated.MetricUnion{
			Type:          metric.TimerType,
			ID:            []byte(testIDName),
			BatchTimerVal: timerValues,
		}
		payload = payloadUnion{
			payloadType: untimedType,
			untimed: untimedPayload{
				metric:    testLargeBatchTimer,
				metadatas: testStagedMetadatas,
			},
		}
		testScope = tally.NewTestScope("", nil)
		iOpts     = instrument.NewOptions().SetMetricsScope(testScope)
		opts      = testOptions().
				SetMaxBatchSize(1000).
				SetMaxTimerBatchSize(10).
				SetInstrumentOptions(iOpts)

		w            = newInstanceWriter(testPlacementInstance, opts).(*writer)
		q            = w.queue.(*queue)
		payloadCount int
	)

	q.writeFn = func(payload []byte) error {
		payloadCount += strings.Count(string(payload), testIDName)
		return nil
	}

	require.NoError(t, w.Write(0, payload))
	require.NoError(t, w.Flush())
	time.Sleep(1 * time.Second) // TODO: remove once queue is sync
	require.NoError(t, w.Close())

	enqueuedCounter := testScope.Snapshot().Counters()["buffers+action=enqueued"]
	require.NotNil(t, enqueuedCounter)
	// Expect 1 byte buffer to be enqueued to write to network,
	// but timer itself should be split to multiple protobuf payloads.
	require.Equal(t, int64(1), enqueuedCounter.Value())
	require.Equal(t, 140, payloadCount)
}

func TestWriterWriteUntimedBatchTimerWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	numValues := 7
	timerValues := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		timerValues[i] = float64(i)
	}
	testLargeBatchTimer := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testLargeBatchTimer"),
		BatchTimerVal: timerValues,
	}

	errTestWrite = errors.New("test write error")
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().
			EncodeMessage(gomock.Any()).
			Return(nil),
		encoder.EXPECT().
			EncodeMessage(gomock.Any()).
			Return(errTestWrite),

		encoder.EXPECT().Truncate(3).Return(nil),
	)
	opts := testOptions().SetMaxTimerBatchSize(3)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testLargeBatchTimer,
			metadatas: testStagedMetadatas,
		},
	}
	require.Equal(t, errTestWrite, w.Write(0, payload))
}

func TestWriterWriteUntimedBatchTimerEnqueueError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestEnqueue := errors.New("test enqueue error")
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().Enqueue(gomock.Any()).Return(errTestEnqueue)

	opts := testOptions().
		SetMaxTimerBatchSize(1).
		SetMaxBatchSize(1)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.queue = queue

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testBatchTimer,
			metadatas: testStagedMetadatas,
		},
	}
	require.Equal(t, errTestEnqueue, w.Write(0, payload))
}

func TestWriterWriteUntimedGauge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           testGauge.Gauge(),
				StagedMetadatas: testStagedMetadatas,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
	)
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testGauge,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, w.Write(0, payload))
}

func TestWriterWriteForwardedWithFlushingZeroSizeBefore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		stream      = protobuf.NewBuffer([]byte{1, 2, 3, 4, 5, 6, 7}, nil)
		enqueuedBuf protobuf.Buffer
	)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(0),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.ForwardedMetricWithMetadataType,
			ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
				ForwardedMetric: testForwarded,
				ForwardMetadata: testForwardMetadata,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
		encoder.EXPECT().Relinquish().Return(stream),
	)
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			enqueuedBuf = buf
			return nil
		})
	w := newInstanceWriter(testPlacementInstance, testOptions().SetMaxBatchSize(3)).(*writer)

	w.queue = queue
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   testForwarded,
			metadata: testForwardMetadata,
		},
	}
	require.NoError(t, w.Write(0, payload))

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7}, enqueuedBuf.Bytes())
}

func TestWriterWriteForwardedWithFlushingPositiveSizeBefore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		stream      = protobuf.NewBuffer([]byte{1, 2, 3, 4, 5, 6, 7}, nil)
		enqueuedBuf protobuf.Buffer
	)
	encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
	gomock.InOrder(
		encoder.EXPECT().Len().Return(3),
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.ForwardedMetricWithMetadataType,
			ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
				ForwardedMetric: testForwarded,
				ForwardMetadata: testForwardMetadata,
			},
		}).Return(nil),
		encoder.EXPECT().Len().Return(7),
		encoder.EXPECT().Relinquish().Return(stream),
	)
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			enqueuedBuf = buf
			return nil
		})
	w := newInstanceWriter(testPlacementInstance, testOptions().SetMaxBatchSize(3)).(*writer)

	w.queue = queue
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   testForwarded,
			metadata: testForwardMetadata,
		},
	}
	require.NoError(t, w.Write(0, payload))

	enc, exists := w.encodersByShard[0]
	require.True(t, exists)
	require.NotNil(t, enc)
	require.Equal(t, 1, len(w.encodersByShard))
	require.Equal(t, []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7}, enqueuedBuf.Bytes())
}

func TestWriterWriteForwardedEncodeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestEncodeMetric := errors.New("error encoding metrics")
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
		encoder.EXPECT().Len().Return(0)
		encoder.EXPECT().EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.ForwardedMetricWithMetadataType,
			ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
				ForwardedMetric: testForwarded,
				ForwardMetadata: testForwardMetadata,
			},
		}).Return(errTestEncodeMetric)
		encoder.EXPECT().Truncate(0).Return(nil)
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}

	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   testForwarded,
			metadata: testForwardMetadata,
		},
	}
	require.Equal(t, errTestEncodeMetric, w.Write(0, payload))
}

func TestWriterWriteForwardedEnqueueError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestEnqueue := errors.New("test enqueue error")
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().Enqueue(gomock.Any()).Return(errTestEnqueue)
	opts := testOptions().
		SetMaxTimerBatchSize(1).
		SetMaxBatchSize(1)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.queue = queue

	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   testForwarded,
			metadata: testForwardMetadata,
		},
	}
	require.Equal(t, errTestEnqueue, w.Write(0, payload))
}

func TestWriterFlushClosed(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.closed.Store(true)
	require.Equal(t, ErrInstanceWriterClosed, w.Flush())
}

func TestWriterFlushPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		enqueueIdx   int
		enqueued     []byte
		errTestFlush = errors.New("test flush error")
	)
	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			enqueued = append(enqueued, buf.Bytes()...)
			enqueueIdx++
			if enqueueIdx == 1 {
				return errTestFlush
			}
			return nil
		}).
		Times(2)
	queue.EXPECT().Flush().MinTimes(1)
	opts := testOptions()
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.queue = queue

	encoderIdx := 0
	w.newLockedEncoderFn = func(protobuf.UnaggregatedOptions) *lockedEncoder {
		encoder := protobuf.NewMockUnaggregatedEncoder(ctrl)
		switch encoderIdx {
		case 0:
			encoder.EXPECT().Len().Return(0)
		case 1:
			encoder.EXPECT().Len().Return(2)
			encoder.EXPECT().Relinquish().Return(protobuf.NewBuffer([]byte{1, 2}, nil))
		case 2:
			encoder.EXPECT().Len().Return(4)
			encoder.EXPECT().Relinquish().Return(protobuf.NewBuffer([]byte{3, 4, 5, 6}, nil))
		}
		encoderIdx++
		return &lockedEncoder{UnaggregatedEncoder: encoder}
	}
	for i := 0; i < 3; i++ {
		w.encodersByShard[uint32(i)] = w.newLockedEncoderFn(opts.EncoderOptions())
	}
	err := w.Flush()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errTestFlush.Error()))
	sort.Slice(enqueued, func(i, j int) bool { return enqueued[i] < enqueued[j] })
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, enqueued)
}

func TestWriterCloseAlreadyClosed(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	w.closed.Store(true)
	require.Equal(t, ErrInstanceWriterClosed, w.Close())
}

func TestWriterCloseSuccess(t *testing.T) {
	w := newInstanceWriter(testPlacementInstance, testOptions()).(*writer)
	require.NoError(t, w.Close())
}

//nolint:dupl
func TestWriterCloseFlushInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	numWrites := 0

	blockCh := make(chan bool)
	ctx := context.Background()
	ctx, cancelFn := context.WithTimeout(ctx, 2*time.Second)
	defer cancelFn()

	slowMockConn := NewMockConn(ctrl)
	slowMockConn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (n int, err error) {
		// notify that the slow writer is about to block
		blockCh <- true
		<-ctx.Done()
		numWrites++
		return len(b), nil
	}).Times(2)
	slowMockConn.EXPECT().SetWriteDeadline(gomock.Any()).Times(2)

	slowWriterDialerFn := func(c context.Context, network string, address string) (net.Conn, error) {
		return slowMockConn, nil
	}

	slowConnOpts := NewConnectionOptions().SetContextDialer(slowWriterDialerFn)
	opts := testOptions().SetConnectionOptions(slowConnOpts)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}

	require.Equal(t, nil, w.Write(0, payload))

	var wg sync.WaitGroup

	// Initiate first flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Flush() //nolint:errcheck
	}()

	// Wait for the first flush to block
	<-blockCh

	// Now buffer up some additional data
	require.Equal(t, nil, w.Write(0, payload))

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Close() //nolint:errcheck
	}()

	// Wait for Close()-flush to busy-loop behind first flush
	// 200ms is enough time for the Close()-flush to
	// start busy-looping since the _closeFlushSoakTimeMs is
	// 100ms
	time.Sleep(200 * time.Millisecond)

	// Unblock the slow writer
	cancelFn()

	// Unblock the Close()-flush
	<-blockCh

	// Wait for first flush and Close() to complete
	wg.Wait()

	assert.Equal(t, 2, numWrites)
}

func TestWriterConcurrentWriteStress(t *testing.T) {
	params := []struct {
		maxInputBatchSize int
		maxTimerBatchSize int
		maxBatchSize      int
	}{
		// High likelihood of counter/gauge encoding triggering a flush in between
		// releasing and re-acquiring locks when encoding large timer batches.
		{
			maxInputBatchSize: 150,
			maxTimerBatchSize: 150,
			maxBatchSize:      1000,
		},
		// Large timer batches.
		{
			maxInputBatchSize: 1000,
			maxTimerBatchSize: 140,
			maxBatchSize:      1440,
		},
	}

	for _, param := range params {
		testWriterConcurrentWriteStress(
			t,
			param.maxInputBatchSize,
			param.maxTimerBatchSize,
			param.maxBatchSize,
		)
	}
}

func testWriterConcurrentWriteStress(
	t *testing.T,
	maxInputBatchSize int,
	maxTimerBatchSize int,
	maxBatchSize int,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		numIter       = 3000
		shard         = uint32(0)
		counters      = make([]unaggregated.Counter, numIter)
		timers        = make([]unaggregated.BatchTimer, numIter)
		gauges        = make([]unaggregated.Gauge, numIter)
		forwarded     = make([]aggregated.ForwardedMetric, numIter)
		passthroughed = make([]aggregated.Metric, numIter)
		resultsLock   sync.Mutex
		results       [][]byte
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
		forwardedVals := []float64{float64(i) - 0.5, float64(i), float64(i) + 0.5}
		forwarded[i] = aggregated.ForwardedMetric{
			Type:      metric.GaugeType,
			ID:        []byte(fmt.Sprintf("forwarded%d", i)),
			TimeNanos: int64(i),
			Values:    forwardedVals,
		}
		passthroughed[i] = aggregated.Metric{
			Type:      metric.GaugeType,
			ID:        []byte(fmt.Sprintf("passthroughed%d", i)),
			TimeNanos: int64(i),
			Value:     float64(i),
		}
	}

	queue := NewMockinstanceQueue(ctrl)
	queue.EXPECT().
		Enqueue(gomock.Any()).
		DoAndReturn(func(buf protobuf.Buffer) error {
			bytes := buf.Bytes()
			cloned := make([]byte, len(bytes))
			copy(cloned, bytes)
			resultsLock.Lock()
			results = append(results, cloned)
			resultsLock.Unlock()
			return nil
		}).
		AnyTimes()
	queue.EXPECT().Flush().MinTimes(1)
	opts := testOptions().
		SetMaxTimerBatchSize(maxTimerBatchSize).
		SetMaxBatchSize(maxBatchSize)
	w := newInstanceWriter(testPlacementInstance, opts).(*writer)
	w.queue = queue

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:       metric.CounterType,
				ID:         counters[i].ID,
				CounterVal: counters[i].Value,
			}
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    mu,
					metadatas: testStagedMetadatas,
				},
			}
			require.NoError(t, w.Write(shard, payload))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:          metric.TimerType,
				ID:            timers[i].ID,
				BatchTimerVal: timers[i].Values,
			}
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    mu,
					metadatas: testStagedMetadatas,
				},
			}
			require.NoError(t, w.Write(shard, payload))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			mu := unaggregated.MetricUnion{
				Type:     metric.GaugeType,
				ID:       gauges[i].ID,
				GaugeVal: gauges[i].Value,
			}
			payload := payloadUnion{
				payloadType: untimedType,
				untimed: untimedPayload{
					metric:    mu,
					metadatas: testStagedMetadatas,
				},
			}
			require.NoError(t, w.Write(shard, payload))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			payload := payloadUnion{
				payloadType: forwardedType,
				forwarded: forwardedPayload{
					metric:   forwarded[i],
					metadata: testForwardMetadata,
				},
			}
			require.NoError(t, w.Write(shard, payload))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIter; i++ {
			payload := payloadUnion{
				payloadType: passthroughType,
				passthrough: passthroughPayload{
					metric:        passthroughed[i],
					storagePolicy: testPassthroughMetadata,
				},
			}
			require.NoError(t, w.Write(shard, payload))
		}
	}()

	wg.Wait()
	w.Flush()

	var (
		resCounters      = make([]unaggregated.Counter, 0, numIter)
		resTimers        = make([]unaggregated.BatchTimer, 0, numIter)
		resGauges        = make([]unaggregated.Gauge, 0, numIter)
		resForwarded     = make([]aggregated.ForwardedMetric, 0, numIter)
		resPassthroughed = make([]aggregated.Metric, 0, numIter)
	)
	for i := 0; i < len(results); i++ {
		buf := bytes.NewBuffer(results[i])
		iter := protobuf.NewUnaggregatedIterator(buf, protobuf.NewUnaggregatedOptions())
		for iter.Next() {
			msgResult := iter.Current()
			switch msgResult.Type {
			case encoding.CounterWithMetadatasType:
				require.Equal(t, testStagedMetadatas, msgResult.CounterWithMetadatas.StagedMetadatas)
				metric := cloneMetric(msgResult.CounterWithMetadatas.Counter.ToUnion())
				resCounters = append(resCounters, metric.Counter())
			case encoding.BatchTimerWithMetadatasType:
				require.Equal(t, testStagedMetadatas, msgResult.BatchTimerWithMetadatas.StagedMetadatas)
				metric := cloneMetric(msgResult.BatchTimerWithMetadatas.BatchTimer.ToUnion())
				resTimers = append(resTimers, metric.BatchTimer())
			case encoding.GaugeWithMetadatasType:
				require.Equal(t, testStagedMetadatas, msgResult.GaugeWithMetadatas.StagedMetadatas)
				metric := cloneMetric(msgResult.GaugeWithMetadatas.Gauge.ToUnion())
				resGauges = append(resGauges, metric.Gauge())
			case encoding.ForwardedMetricWithMetadataType:
				require.Equal(t, testForwardMetadata, msgResult.ForwardedMetricWithMetadata.ForwardMetadata)
				metric := cloneForwardedMetric(msgResult.ForwardedMetricWithMetadata.ForwardedMetric)
				resForwarded = append(resForwarded, metric)
			case encoding.PassthroughMetricWithMetadataType:
				require.Equal(t, testPassthroughMetadata, msgResult.PassthroughMetricWithMetadata.StoragePolicy)
				metric := clonePassthroughedMetric(msgResult.PassthroughMetricWithMetadata.Metric)
				resPassthroughed = append(resPassthroughed, metric)
			default:
				require.Fail(t, "unrecognized message type %v", msgResult.Type)
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

	// Sort forwarded for comparison purposes.
	sort.Slice(forwarded, func(i, j int) bool {
		return bytes.Compare(forwarded[i].ID, forwarded[j].ID) < 0
	})
	sort.Slice(resForwarded, func(i, j int) bool {
		return bytes.Compare(resForwarded[i].ID, resForwarded[j].ID) < 0
	})
	require.Equal(t, forwarded, resForwarded)
}

func TestRefCountedWriter(t *testing.T) {
	opts := testOptions()
	w := newRefCountedWriter(testPlacementInstance, opts)
	w.IncRef()

	require.False(t, w.instanceWriter.(*writer).closed.Load())
	w.DecRef()
	require.True(t, clock.WaitUntil(func() bool {
		wr := w.instanceWriter.(*writer)
		return wr.closed.Load()
	}, 3*time.Second))
}

func cloneMetric(m unaggregated.MetricUnion) unaggregated.MetricUnion {
	mu := m
	clonedID := make(id.RawID, len(m.ID))
	copy(clonedID, m.ID)
	mu.ID = clonedID
	if m.Type == metric.TimerType {
		clonedTimerVal := make([]float64, len(m.BatchTimerVal))
		copy(clonedTimerVal, m.BatchTimerVal)
		mu.BatchTimerVal = clonedTimerVal
	}
	return mu
}

func cloneForwardedMetric(m aggregated.ForwardedMetric) aggregated.ForwardedMetric {
	cloned := m
	cloned.ID = append([]byte(nil), m.ID...)
	cloned.Values = append([]float64(nil), m.Values...)
	return cloned
}

func clonePassthroughedMetric(m aggregated.Metric) aggregated.Metric {
	cloned := m
	cloned.ID = append([]byte(nil), m.ID...)
	cloned.Value = m.Value
	return cloned
}
