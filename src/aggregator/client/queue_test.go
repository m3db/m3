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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
)

func TestInstanceQueueEnqueueClosed(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	queue.writeFn = func([]byte) error { return nil }
	queue.closed.Store(true)

	require.Equal(t, errInstanceQueueClosed, queue.Enqueue(testNewBuffer(nil)))
}

func TestInstanceQueueEnqueueQueueFullDropCurrent(t *testing.T) {
	opts := testOptions().
		SetInstanceQueueSize(2).
		SetQueueDropType(DropCurrent)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

	var result []byte
	queue.writeFn = func(payload []byte) error {
		result = payload
		return nil
	}
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42, 43, 44})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{45, 46, 47})))
	require.Equal(t, errWriterQueueFull, queue.Enqueue(testNewBuffer([]byte{42})))
	queue.Flush()
	require.EqualValues(t, []byte{42, 43, 44, 45, 46, 47}, result)
}

func TestInstanceQueueEnqueueQueueFullDropOldest(t *testing.T) {
	opts := testOptions().
		SetInstanceQueueSize(4)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

	var result []byte
	queue.writeFn = func(payload []byte) error {
		result = payload
		return nil
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42, 43, 44})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{45, 46, 47})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{1, 2, 3})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{1})))

	queue.Flush()
	require.EqualValues(t, []byte{
		42, 43, 44, 45, 46, 47, 1, 2, 3, 1,
	}, result)

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{1, 2, 3})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42, 43, 44})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{45, 46, 47})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{1})))

	queue.Flush()

	require.EqualValues(t, []byte{
		42, 42, 43, 44, 45, 46, 47, 1,
	}, result)
}

func TestInstanceQueueEnqueueQueueFullBytesDropCurrent(t *testing.T) {
	opts := testOptions().SetQueueDropType(DropCurrent).SetInstanceMaxQueueSizeBytes(8)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

	var result []byte
	queue.writeFn = func(payload []byte) error {
		result = payload
		return nil
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{1, 2, 3})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{4, 5, 6})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{7, 8, 9})))

	require.Equal(t, errWriterQueueFull, queue.Enqueue(testNewBuffer([]byte{42})))

	queue.Flush()

	require.EqualValues(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, result)
}

func TestInstanceQueueEnqueueQueueFullBytesDropOldest(t *testing.T) {
	tests := []struct {
		name           string
		queueSizeBytes int
		queueSizeItems int
		toEnqueue      []protobuf.Buffer
		expected       []byte
	}{
		{
			name:           "last one removes all previous enqueued buffers",
			queueSizeBytes: 5,
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{
				testNewBuffer([]byte{42}),
				testNewBuffer([]byte{42, 43, 44}),
				testNewBuffer([]byte{45, 46, 47}),
				testNewBuffer([]byte{1, 2, 3, 4, 5}),
				testNewBuffer([]byte{1}),
			},
			expected: []byte{1},
		},
		{
			name:           "last two buffers remain",
			queueSizeBytes: 5,
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{
				testNewBuffer([]byte{}),
				testNewBuffer([]byte{1, 2, 3}),
				testNewBuffer([]byte{42}),
				testNewBuffer([]byte{}),
				testNewBuffer([]byte{42, 43, 44}),
				testNewBuffer([]byte{45, 46, 47}),
				testNewBuffer([]byte{1}),
			},
			expected: []byte{45, 46, 47, 1},
		},
		{
			name:           "zero length buffer enqueued",
			queueSizeBytes: 5,
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{},
			expected:  []byte{},
		},
		{
			name:           "big last buffer expands queue beyond limit",
			queueSizeBytes: 5,
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{
				testNewBuffer([]byte{1, 2, 3}),
				testNewBuffer([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}),
			},
			expected: []byte{1, 2, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		},
		{
			name:           "last one dequeues all others",
			queueSizeBytes: 5,
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{
				testNewBuffer([]byte{1}),
				testNewBuffer([]byte{1}),
				testNewBuffer([]byte{1}),
				testNewBuffer([]byte{1}),
				testNewBuffer([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}),
				testNewBuffer([]byte{2}),
			},
			expected: []byte{2},
		},
		{
			name: "default queue sizes",
			// default queueSizeBytes (0) no-limit
			// default queueSizeItems (128)
			toEnqueue: []protobuf.Buffer{
				testNewBuffer([]byte{}),
				testNewBuffer([]byte{1, 2, 3}),
				testNewBuffer([]byte{42}),
				testNewBuffer([]byte{}),
				testNewBuffer([]byte{42, 43, 44}),
				testNewBuffer([]byte{45, 46, 47}),
				testNewBuffer([]byte{1}),
			},
			expected: []byte{1, 2, 3, 42, 42, 43, 44, 45, 46, 47, 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := testOptions()
			if test.queueSizeBytes > 0 {
				opts = opts.SetInstanceMaxQueueSizeBytes(test.queueSizeBytes)
			}
			if test.queueSizeItems > 0 {
				opts = opts.SetInstanceQueueSize(test.queueSizeItems)
			}

			queue := newInstanceQueue(testPlacementInstance, opts).(*queue)

			result := []byte{}
			queue.writeFn = func(payload []byte) error {
				result = payload
				return nil
			}

			for _, input := range test.toEnqueue {
				require.NoError(t, queue.Enqueue(input),
					"Enqueue failed for test %q", test.name)
			}

			queue.Flush()

			require.EqualValues(t, test.expected, result,
				"Enqueue expectation failed for test %q", test.name)
		})
	}
}

func TestInstanceQueueEnqueueLargeBuffers(t *testing.T) {
	var (
		opts = testOptions().
			SetInstanceQueueSize(4)
		queue        = newInstanceQueue(testPlacementInstance, opts).(*queue)
		largeBuf     = [_queueMaxWriteBufSize * 2]byte{}
		bytesWritten int
		timesWritten int
	)

	queue.writeFn = func(payload []byte) error {
		bytesWritten += len(payload)
		timesWritten++
		return nil
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	require.NoError(t, queue.Enqueue(testNewBuffer(largeBuf[:])))
	queue.Flush()
	require.Equal(t, len(largeBuf)+3, bytesWritten)
	require.Equal(t, 2, timesWritten)

	timesWritten, bytesWritten = 0, 0
	require.NoError(t, queue.Enqueue(testNewBuffer(largeBuf[:])))
	queue.Flush()
	require.Equal(t, len(largeBuf), bytesWritten)
	require.Equal(t, 1, timesWritten)
}

func TestInstanceQueueEnqueueSuccessDrainSuccess(t *testing.T) {
	opts := testOptions().SetMaxBatchSize(1)
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	var (
		res []byte
	)

	ready := make(chan struct{}, 1)
	queue.writeFn = func(data []byte) error {
		defer func() {
			ready <- struct{}{}
		}()
		res = data
		return nil
	}

	data := []byte("foobar")
	require.NoError(t, queue.Enqueue(testNewBuffer(data)))

	queue.Flush()
	<-ready

	require.Equal(t, data, res)
}

func TestInstanceQueueEnqueueSuccessDrainError(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	drained := make(chan struct{}, 1)
	queue.writeFn = func(data []byte) error {
		defer func() {
			drained <- struct{}{}
		}()
		return errTestWrite
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{42})))
	queue.Flush()
	// Wait for the queue to be drained.
	<-drained
}

func TestInstanceQueueEnqueueSuccessWriteError(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	done := make(chan struct{}, 1)
	queue.writeFn = func(data []byte) error {
		err := queue.conn.Write(data)
		done <- struct{}{}
		return err
	}

	require.NoError(t, queue.Enqueue(testNewBuffer([]byte{0x1, 0x2})))
	queue.Flush()
	// Wait for the queue to be drained.
	<-done
}

func TestInstanceQueueCloseAlreadyClosed(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	queue.closed.Store(true)

	require.Equal(t, errInstanceQueueClosed, queue.Close())
}

func TestInstanceQueueCloseSuccess(t *testing.T) {
	opts := testOptions()
	queue := newInstanceQueue(testPlacementInstance, opts).(*queue)
	require.NoError(t, queue.Close())
	require.True(t, queue.closed.Load())
	require.Error(t, queue.Enqueue(testNewBuffer([]byte("foo"))))
}

func TestInstanceQueueSizeIsPowerOfTwo(t *testing.T) {
	for _, tt := range []struct {
		size     int
		expected int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{42, 64},
		{123, 128},
	} {
		opts := testOptions().SetInstanceQueueSize(tt.size)
		q := newInstanceQueue(testPlacementInstance, opts).(*queue)
		require.Equal(t, tt.expected, cap(q.buf.b))
	}
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

func TestRoundUpToPowerOfTwo(t *testing.T) {
	for _, tt := range []struct {
		in, out int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{33, 64},
		{42, 64},
	} {
		assert.Equal(t, tt.out, roundUpToPowerOfTwo(tt.in))
	}
}

func testNewBuffer(data []byte) protobuf.Buffer { return protobuf.NewBuffer(data, nil) }
