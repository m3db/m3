// Copyright (c) 2025 Uber Technologies, Inc.
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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewInstanceQueueSingleConnection(t *testing.T) {
	opts := testOptions().SetConnectionsPerInstance(1)
	q := newInstanceQueue(testPlacementInstance, opts)

	// Should return a single queue, not a multiQueue
	_, ok := q.(*multiQueue)
	require.False(t, ok, "expected single queue for ConnectionsPerInstance=1")

	_, ok = q.(*queue)
	require.True(t, ok, "expected *queue type for ConnectionsPerInstance=1")
}

func TestNewInstanceQueueMultipleConnections(t *testing.T) {
	opts := testOptions().SetConnectionsPerInstance(4)
	q := newInstanceQueue(testPlacementInstance, opts)

	// Should return a multiQueue
	mq, ok := q.(*multiQueue)
	require.True(t, ok, "expected multiQueue for ConnectionsPerInstance=4")
	require.Len(t, mq.queues, 4, "expected 4 child queues")
}

func TestHashRouterConsistency(t *testing.T) {
	router := hashRouter{}
	buf := testNewBuffer([]byte("test-metric"))
	require.Equal(t, router.Select(buf, 4), router.Select(buf, 4))
}

func TestMultiQueueEnqueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueue1 := NewMockinstanceQueue(ctrl)
	mockQueue2 := NewMockinstanceQueue(ctrl)
	mockQueue3 := NewMockinstanceQueue(ctrl)

	mq := &multiQueue{
		queues: []instanceQueue{mockQueue1, mockQueue2, mockQueue3},
		router: hashRouter{},
	}

	buf := testNewBuffer([]byte("test-metric"))
	idx := mq.router.Select(buf, len(mq.queues))

	switch idx {
	case 0:
		mockQueue1.EXPECT().Enqueue(buf).Return(nil)
	case 1:
		mockQueue2.EXPECT().Enqueue(buf).Return(nil)
	case 2:
		mockQueue3.EXPECT().Enqueue(buf).Return(nil)
	}

	err := mq.Enqueue(buf)
	require.NoError(t, err)
}

func TestMultiQueueSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueue1 := NewMockinstanceQueue(ctrl)
	mockQueue2 := NewMockinstanceQueue(ctrl)
	mockQueue3 := NewMockinstanceQueue(ctrl)

	mockQueue1.EXPECT().Size().Return(5)
	mockQueue2.EXPECT().Size().Return(10)
	mockQueue3.EXPECT().Size().Return(3)

	mq := &multiQueue{
		queues: []instanceQueue{mockQueue1, mockQueue2, mockQueue3},
		router: hashRouter{},
	}

	size := mq.Size()
	require.Equal(t, 18, size, "expected sum of all queue sizes")
}

func TestMultiQueueClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("all succeed", func(t *testing.T) {
		mockQueue1 := NewMockinstanceQueue(ctrl)
		mockQueue2 := NewMockinstanceQueue(ctrl)

		mockQueue1.EXPECT().Close().Return(nil)
		mockQueue2.EXPECT().Close().Return(nil)

		mq := &multiQueue{
			queues: []instanceQueue{mockQueue1, mockQueue2},
			router: hashRouter{},
		}

		err := mq.Close()
		require.NoError(t, err)
	})

	t.Run("collects all errors", func(t *testing.T) {
		mockQueue1 := NewMockinstanceQueue(ctrl)
		mockQueue2 := NewMockinstanceQueue(ctrl)
		mockQueue3 := NewMockinstanceQueue(ctrl)

		err1 := errors.New("error from queue 1")
		err2 := errors.New("error from queue 3")

		mockQueue1.EXPECT().Close().Return(err1)
		mockQueue2.EXPECT().Close().Return(nil)
		mockQueue3.EXPECT().Close().Return(err2)

		mq := &multiQueue{
			queues: []instanceQueue{mockQueue1, mockQueue2, mockQueue3},
			router: hashRouter{},
		}

		err := mq.Close()
		require.Error(t, err)
		// MultiError should contain both errors
		require.Contains(t, err.Error(), "error from queue 1")
		require.Contains(t, err.Error(), "error from queue 3")
	})

	t.Run("closes all queues even if some fail", func(t *testing.T) {
		mockQueue1 := NewMockinstanceQueue(ctrl)
		mockQueue2 := NewMockinstanceQueue(ctrl)
		mockQueue3 := NewMockinstanceQueue(ctrl)

		err1 := errors.New("error from queue 1")

		// All queues should be called, even if first one fails
		mockQueue1.EXPECT().Close().Return(err1)
		mockQueue2.EXPECT().Close().Return(nil)
		mockQueue3.EXPECT().Close().Return(nil)

		mq := &multiQueue{
			queues: []instanceQueue{mockQueue1, mockQueue2, mockQueue3},
			router: hashRouter{},
		}

		err := mq.Close()
		require.Error(t, err)
	})
}

func TestMultiQueueFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueue1 := NewMockinstanceQueue(ctrl)
	mockQueue2 := NewMockinstanceQueue(ctrl)
	mockQueue3 := NewMockinstanceQueue(ctrl)

	// All queues should be flushed sequentially
	gomock.InOrder(
		mockQueue1.EXPECT().Flush(),
		mockQueue2.EXPECT().Flush(),
		mockQueue3.EXPECT().Flush(),
	)

	mq := &multiQueue{
		queues: []instanceQueue{mockQueue1, mockQueue2, mockQueue3},
		router: hashRouter{},
	}

	mq.Flush()
}

func TestMultiQueueDistribution(t *testing.T) {
	// Test that different metrics get distributed across queues
	opts := testOptions().SetConnectionsPerInstance(4)
	mq := newInstanceQueue(testPlacementInstance, opts).(*multiQueue)

	// Track which queues receive data
	queueHits := make(map[int]int)

	// Generate 100 different metric buffers and track distribution
	for i := 0; i < 100; i++ {
		buf := testNewBuffer([]byte{byte(i), byte(i >> 8)})
		idx := mq.router.Select(buf, len(mq.queues))
		queueHits[idx]++
	}

	// We should see distribution across multiple queues
	// (not guaranteed to hit all due to hash collisions, but should hit > 1)
	require.Greater(t, len(queueHits), 1, "expected distribution across multiple queues")
}

func TestNewInstanceMultiQueueZeroConnections(t *testing.T) {
	opts := testOptions().SetConnectionsPerInstance(0)
	mq := newInstanceMultiQueue(testPlacementInstance, opts).(*multiQueue)
	require.Len(t, mq.queues, 1, "expected at least 1 queue")
}
