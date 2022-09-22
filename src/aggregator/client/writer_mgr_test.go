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
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/clock"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
)

var (
	testPlacementInstance = placement.NewInstance().
		SetID("testInstanceID").
		SetEndpoint("testInstanceAddress")
)

func TestWriterManagerAddInstancesClosed(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.closed = true
	mgr.Unlock()
	require.Equal(t, errInstanceWriterManagerClosed, mgr.AddInstances(nil))
}

func TestWriterManagerAddInstancesSingleRef(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())

	// Add instance lists twice and assert the writer refcount matches expectation.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	}
	mgr.Lock()
	require.Equal(t, 1, len(mgr.writers))
	w, exists := mgr.writers[testPlacementInstance.ID()]
	mgr.Unlock()
	require.True(t, exists)
	require.Equal(t, int32(2), w.refCount.n)
}

// TestWriterManagerMultipleWriters tries to recreate the scenario that
// has multiple writers out of which one is slow. We have had multiple
// incidents where one slow writer blocks all the other writers in the
// aggregator client and cascades into a variety of issues including
// dropped metrics, OOM etc.
// How does this test mimic the slow writer?
// It first creates a custom dialer for the TcpClient and
// and makes the writeFn block on the context.Context. An instance created
// with this overridden connection options struct creates a slow writer.
// After the writer/instance is created, we re-override the write with
// another custom dialer and replace the write with non-blocking
// instructions. These writers are called normal writers. Instances added after
// updating the opts with the re-overridden dialer creates normal writers.
// The test creates several normal writers and one slow writer.
// First it writes and flushes to the slow writer and get it to block.
// It then begins a loop of writing one payload to every writer: slow and normal.
// We initiate every write-loop in a separate goroutine.
// We keep track of completed writes of the normal clients by way of the
// counter writesCompleted.
// As soon as all write-loops are done, meaning that all writers have
// finished writing all that had to write, the context is canceled. This unblocks
// the slow writer and then we perform the necessary validations.
func TestWriterManagerMultipleWriters(t *testing.T) {
	ctrl := xtest.NewController(t)
	waitForSlowWriter := make(chan bool)

	var writesCompleted atomic.Int32
	var flushInProgressCnt atomic.Int32

	const (
		numIterations      = 10
		numNormalInstances = 16
	)

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	slowMockConn := NewMockConn(ctrl)
	slowMockConn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (n int, err error) {
		// Block till all normal writers have finished writing
		// all write loops below
		waitForSlowWriter <- true
		<-ctx.Done()
		return len(b), nil
	}).AnyTimes()
	slowMockConn.EXPECT().SetWriteDeadline(gomock.Any()).AnyTimes()

	slowWriterDialerFn := func(c context.Context, network string, address string) (net.Conn, error) {
		return slowMockConn, nil
	}

	normalMockConn := NewMockConn(ctrl)
	normalMockConn.EXPECT().Write(gomock.Any()).DoAndReturn(func(b []byte) (n int, err error) {
		writesCompleted.Inc()
		return len(b), nil
	}).AnyTimes()
	normalMockConn.EXPECT().SetWriteDeadline(gomock.Any()).AnyTimes()

	normalWriterDialerFn := func(ctx context.Context, network string, address string) (net.Conn, error) {
		return normalMockConn, nil
	}

	// Override opts for slow writer
	slowConnOpts := testConnectionOptions().SetContextDialer(slowWriterDialerFn)

	opts := testOptions().SetConnectionOptions(slowConnOpts).SetFlushWorkerCount(256)
	mgr := mustMakeInstanceWriterManager(opts)

	slowInstance := placement.NewInstance().
		SetID("slowTestID").
		SetEndpoint("SlowTestEp")

	// Add slow writer/instance
	require.NoError(t, mgr.AddInstances([]placement.Instance{slowInstance}))

	// Re-override opts for normal writers
	mgr.opts = mgr.opts.SetConnectionOptions(testConnectionOptions().SetContextDialer(normalWriterDialerFn))

	instances := []placement.Instance{}
	for i := 0; i < numNormalInstances; i++ {
		instances = append(instances, placement.NewInstance().
			SetID("testID"+strconv.Itoa(i)).
			SetEndpoint("testEp"+strconv.Itoa(i)),
		)
	}

	// Create normal writers
	require.NoError(t, mgr.AddInstances(instances))

	// start slow writer asynchronously and wait till it blocks
	err := mgr.Write(slowInstance, 0, testCounterPayloadUnion(0))
	require.NoError(t, err)

	go mgr.Flush() //nolint:errcheck
	<-waitForSlowWriter

	// Now the following write loop which contains writes over
	// the slow writer and normal writers should be non-blocking.
	var wg sync.WaitGroup
	for i := 0; i < numIterations; i++ {
		wg.Add(1)
		go func(iterationID int64) {
			defer wg.Done()

			payload := testCounterPayloadUnion(iterationID)
			// The following Write() to the slow instance should increment
			// flushInProgressCnt during Flush()
			err := mgr.Write(slowInstance, 0, payload)
			require.NoError(t, err)

			for _, instance := range instances {
				err := mgr.Write(instance, 0, payload)
				require.NoError(t, err)
			}

			err = mgr.Flush()
			if err != nil {
				require.True(t, strings.Contains(err.Error(), ErrFlushInProgress.Error()))
				flushInProgressCnt.Inc()
			}
		}(int64(i))
	}

	wg.Wait()

	// Now that all normal writers have finished writing
	// cancel the slow writer and compare write counts
	// to validate.
	cancelFn()

	// Unfortunately we cannot compare the write counts because due to the
	// unpredictable nature of how the goroutines race against each other
	// some normal writes could be combined into one flush. Thus making the
	// actual count less than the expected count.
	require.Equal(t, int32(numIterations), flushInProgressCnt.Load())
}

func TestWriterManagerRemoveInstancesClosed(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.closed = true
	mgr.Unlock()
	require.Equal(t, errInstanceWriterManagerClosed, mgr.RemoveInstances(nil))
}

func TestWriterManagerRemoveInstancesSuccess(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())

	// Add instance lists twice.
	for i := 0; i < 2; i++ {
		require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	}
	mgr.Lock()
	require.Equal(t, 1, len(mgr.writers))
	mgr.Unlock()

	// Remove the instance list once and assert they are not closed.
	require.NoError(t, mgr.RemoveInstances([]placement.Instance{testPlacementInstance}))

	mgr.Lock()
	require.Equal(t, 1, len(mgr.writers))
	w := mgr.writers[testPlacementInstance.ID()].instanceWriter.(*writer)
	require.False(t, w.closed.Load())
	mgr.Unlock()

	// Remove the instance list again and assert the writer is now removed.
	nonexistent := placement.NewInstance().
		SetID("nonexistent").
		SetEndpoint("nonexistentAddress")
	toRemove := append([]placement.Instance{nonexistent, testPlacementInstance})
	require.NoError(t, mgr.RemoveInstances(toRemove))
	require.Equal(t, 0, len(mgr.writers))
	require.True(t, clock.WaitUntil(func() bool {
		return w.closed.Load()
	}, 3*time.Second))
}

func TestWriterManagerRemoveInstancesNonBlocking(t *testing.T) {
	var (
		opts = testOptions().SetInstanceQueueSize(200)
		mgr  = mustMakeInstanceWriterManager(opts)
	)
	require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))

	mgr.Lock()
	require.Equal(t, 1, len(mgr.writers))
	w := mgr.writers[testPlacementInstance.ID()].instanceWriter.(*writer)

	w.queue.(*queue).writeFn = func([]byte) error {
		time.Sleep(time.Second)
		return nil
	}
	mgr.Unlock()

	data := []byte("foo")
	for i := 0; i < opts.InstanceQueueSize(); i++ {
		require.NoError(t, w.queue.Enqueue(testNewBuffer(data)))
	}

	go mgr.RemoveInstances([]placement.Instance{testPlacementInstance})
	require.True(t, clock.WaitUntil(func() bool {
		mgr.Lock()
		defer mgr.Unlock()
		return len(mgr.writers) == 0
	}, 3*time.Second))
}

func TestWriterManagerWriteUntimedClosed(t *testing.T) {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.closed = true
	mgr.Unlock()
	err := mgr.Write(testPlacementInstance, 0, payload)
	require.Equal(t, errInstanceWriterManagerClosed, err)
}

func TestWriterManagerWriteUntimedNoInstances(t *testing.T) {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	mgr := mustMakeInstanceWriterManager(testOptions())
	err := mgr.Write(testPlacementInstance, 0, payload)
	require.Error(t, err)
	require.NoError(t, mgr.Close())
}

func TestWriterManagerWriteUntimedSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instances = []placement.Instance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
		shardRes   uint32
		payloadRes payloadUnion
	)
	writer := NewMockinstanceWriter(ctrl)
	writer.EXPECT().QueueSize().AnyTimes()
	writer.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			shard uint32,
			payload payloadUnion,
		) error {
			shardRes = shard
			payloadRes = payload
			return nil
		})
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer,
	}
	mgr.Unlock()

	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    testCounter,
			metadatas: testStagedMetadatas,
		},
	}
	require.NoError(t, mgr.Write(testPlacementInstance, 0, payload))
	mgr.Lock()
	assert.Equal(t, 1, len(mgr.writers))
	mgr.Unlock()
	require.Equal(t, uint32(0), shardRes)
	require.Equal(t, untimedType, payloadRes.payloadType)
	require.Equal(t, testCounter, payloadRes.untimed.metric)
	require.Equal(t, testStagedMetadatas, payloadRes.untimed.metadatas)
}

func TestWriterManagerFlushClosed(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.closed = true
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Flush())
}

func TestWriterManagerFlushPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		numFlushes atomic.Int64
		instances  = []placement.Instance{
			testPlacementInstance,
			placement.NewInstance().
				SetID("foo").
				SetEndpoint("fooAddr"),
		}
	)

	writer1 := NewMockinstanceWriter(ctrl)
	writer1.EXPECT().QueueSize().AnyTimes()
	writer1.EXPECT().Write(gomock.Any(), gomock.Any())
	writer1.EXPECT().
		Flush().
		DoAndReturn(func() error {
			numFlushes.Inc()
			return nil
		})
	errTestFlush := errors.New("test flush error")
	writer2 := NewMockinstanceWriter(ctrl)
	writer2.EXPECT().QueueSize().AnyTimes()
	writer2.EXPECT().Write(gomock.Any(), gomock.Any())
	writer2.EXPECT().
		Flush().
		DoAndReturn(func() error {
			return errTestFlush
		})
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.writers[instances[0].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer1,
	}
	mgr.writers[instances[1].ID()] = &refCountedWriter{
		refCount:       refCount{n: 1},
		instanceWriter: writer2,
	}
	mgr.Unlock()
	mgr.Write(instances[0], 0, payloadUnion{}) //nolint:errcheck
	mgr.Write(instances[1], 0, payloadUnion{}) //nolint:errcheck
	err := mgr.Flush()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errTestFlush.Error()))
	require.Equal(t, int64(1), numFlushes.Load())
}

func TestWriterManagerCloseAlreadyClosed(t *testing.T) {
	mgr := mustMakeInstanceWriterManager(testOptions())
	mgr.Lock()
	mgr.closed = true
	mgr.Unlock()
	require.Equal(t, errInstanceWriterManagerClosed, mgr.Close())
}

func TestWriterManagerCloseSuccess(t *testing.T) {
	// TODO: other tests don't clean up properly, and pool has no Shutdown method
	defer goleak.VerifyNone(
		t,
		goleak.IgnoreCurrent(),
		goleak.IgnoreTopFunction("github.com/m3db/m3/src/x/sync.(*pooledWorkerPool).spawnWorker.func1"),
	)

	mgr := mustMakeInstanceWriterManager(testOptions())

	// Add instance list and close.
	require.NoError(t, mgr.AddInstances([]placement.Instance{testPlacementInstance}))
	require.NoError(t, mgr.Close())
	mgr.Lock()
	require.True(t, mgr.closed)
	mgr.Unlock()

	require.True(t, clock.WaitUntil(func() bool {
		for _, w := range mgr.writers {
			wr := w.instanceWriter.(*writer)
			if !wr.closed.Load() {
				return false
			}
		}
		return true
	}, 3*time.Second))
}

func mustMakeInstanceWriterManager(opts Options) *writerManager {
	wm, err := newInstanceWriterManager(opts)
	if err != nil {
		panic(err)
	}

	return wm.(*writerManager)
}

func testCounterPayloadUnion(val int64) payloadUnion {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric: unaggregated.MetricUnion{
				Type:       metric.CounterType,
				ID:         []byte("foo"),
				CounterVal: val,
			},
			metadatas: testStagedMetadatas,
		},
	}

	return payload
}
