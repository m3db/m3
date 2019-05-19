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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestInstanceQueueEnqueueClosed(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	queue.writeFn = func([]byte) error { return nil }
	queue.closed = true

	require.Equal(t, errInstanceQueueClosed, queue.Enqueue(testNewBuffer(nil)))
}

func TestInstanceQueueEnqueueQueueFullDropCurrent(t *testing.T) {
	opts := testOptions().
		SetInstanceQueueSize(1).
		SetQueueDropType(DropCurrent).
		SetBatchFlushDeadline(1 * time.Microsecond).
		SetMaxBatchSize(1)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

	ready := make(chan struct{})
	// Fill up the queue and park the draining goroutine so the queue remains full.
	queue.writeFn = func([]byte) error {
		ready <- struct{}{}
		select {}
	}

	queue.bufCh <- testNewBuffer([]byte{42, 42, 42})
	queue.bufCh <- testNewBuffer([]byte{42, 42, 42})
	queue.bufCh <- testNewBuffer([]byte{42, 42, 42})
	<-ready
	require.Equal(t, errWriterQueueFull, queue.Enqueue(testNewBuffer([]byte{42})))
}

func TestInstanceQueueEnqueueQueueFullDropOldest(t *testing.T) {
	opts := testOptions().
		SetInstanceQueueSize(1).
		SetBatchFlushDeadline(1 * time.Microsecond).
		SetMaxBatchSize(1)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

	ready := make(chan struct{})
	// Fill up the queue and park the draining goroutine so the queue remains full
	// until the enqueueing goroutine pulls a buffer off the channel.
	queue.writeFn = func([]byte) error {
		ready <- struct{}{}
		select {}
	}

	queue.bufCh <- testNewBuffer([]byte{42})
	queue.bufCh <- testNewBuffer([]byte{42})
	<-ready
	require.NoError(t, queue.Enqueue(testNewBuffer(nil)))
}

func TestInstanceQueueEnqueueSuccessDrainSuccess(t *testing.T) {
	opts := testOptions().SetMaxBatchSize(1)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	var (
		res []byte
	)

	ready := make(chan struct{})
	queue.writeFn = func(data []byte) error {
		defer func() {
			ready <- struct{}{}
		}()
		res = data
		return nil
	}

	data := []byte("foobar")
	require.NoError(t, queue.Enqueue(testNewBuffer(data)))

	<-ready
	require.Equal(t, data, res)
}

func TestInstanceQueueDrainBatching(t *testing.T) {
	var (
		res     []byte
		resLock sync.Mutex
	)

	newBatchedTestQueue := func(flushDeadline time.Duration, batchSize int) *queue {
		res = []byte{}
		opts := testOptions().
			SetBatchFlushDeadline(flushDeadline).
			SetMaxBatchSize(batchSize)

		queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

		queue.writeFn = func(data []byte) error {
			resLock.Lock()
			res = append(res, data...)
			resLock.Unlock()
			return nil
		}
		return queue
	}

	// Test batching by size
	data := []byte("foobar")
	expected := []byte("foobarfoobarfoobar")
	queue := newBatchedTestQueue(500*time.Millisecond, 3*len(data))
	assert.NoError(t, queue.Enqueue(testNewBuffer(data)))
	assert.NoError(t, queue.Enqueue(testNewBuffer(data)))
	assert.NoError(t, queue.Enqueue(testNewBuffer(data)))

	// Wait for the queue to be drained.
	for i := 0; i <= 5; i++ {
		resLock.Lock()
		if len(res) == len(expected) {
			resLock.Unlock()
			break
		}
		resLock.Unlock()
		// Total sleep must be less than flush deadline
		time.Sleep(5 * time.Millisecond)
	}

	assert.Equal(t, expected, res)

	// Test batching by time
	queue = newBatchedTestQueue(40*time.Millisecond, 10000)

	assert.NoError(t, queue.Enqueue(testNewBuffer(data)))
	assert.NoError(t, queue.Enqueue(testNewBuffer(data)))

	time.Sleep(20 * time.Millisecond)
	resLock.Lock()
	assert.Equal(t, []byte{}, res)
	resLock.Unlock()

	time.Sleep(25 * time.Millisecond)

	resLock.Lock()
	assert.Equal(t, []byte("foobarfoobar"), res)
	resLock.Unlock()
}

func TestInstanceQueueEnqueueSuccessDrainError(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	drained := make(chan struct{})
	queue.writeFn = func(data []byte) error {
		defer func() {
			drained <- struct{}{}
		}()
		return errTestWrite
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))

	// Wait for the queue to be drained.
	<-drained
}

func TestInstanceQueueEnqueueSuccessWriteError(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	done := make(chan struct{})
	queue.writeFn = func(data []byte) error {
		err := queue.conn.Write(data)
		done <- struct{}{}
		return err
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{0x1, 0x2})))

	// Wait for the queue to be drained.
	<-done
}

func TestInstanceQueueCloseAlreadyClosed(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	queue.closed = true

	require.Equal(t, errInstanceQueueClosed, queue.Close())
}

func TestInstanceQueueCloseSuccess(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	require.NoError(t, queue.Close())
	require.True(t, queue.closed)
	_, ok := <-queue.bufCh
	require.False(t, ok)
}

func TestDropTypeUnmarshalYAML(t *testing.T) {
	type S struct {
		A DropType
	}

	tests := []struct {
		input    []byte
		expected DropType
	}{
		{
			input:    []byte("a: oldest\n"),
			expected: DropOldest,
		},
		{
			input:    []byte("a: current\n"),
			expected: DropCurrent,
		},
	}

	for _, test := range tests {
		var s S
		err := yaml.Unmarshal(test.input, &s)
		require.NoError(t, err)
		assert.Equal(t, test.expected, s.A)
	}
}

func testNewBuffer(data []byte) protobuf.Buffer { return protobuf.NewBuffer(data, nil) }
