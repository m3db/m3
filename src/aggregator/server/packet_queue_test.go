// Copyright (c) 2016 Uber Technologies, Inc.
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

package server

import (
	"fmt"
	"testing"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func testPacketQueue(size int) *packetQueue {
	return newPacketQueue(size, instrument.NewOptions())
}

func TestPacketQueueEnqueue(t *testing.T) {
	size := 10
	q := testPacketQueue(size)

	// Enqueue up to the queue size
	for i := 1; i <= size; i++ {
		q.Enqueue(packet{})
		require.Equal(t, i, q.Len())
	}

	// Enqueue one more and assert the packet gets dropped
	q.Enqueue(packet{})
	require.Equal(t, size, q.Len())
}

func TestPacketQueueDequeue(t *testing.T) {
	var (
		expected []packet
		results  []packet
		size     = 10
		q        = testPacketQueue(size)
	)
	for i := 1; i <= size; i++ {
		p := packet{
			metric: unaggregated.MetricUnion{
				Type:       unaggregated.CounterType,
				ID:         []byte(fmt.Sprintf("%d", i)),
				CounterVal: int64(i),
			},
			policies: policy.DefaultVersionedPolicies,
		}
		expected = append(expected, p)
		q.Enqueue(p)
	}

	// Dequeue until the queue is empty
	for i := 1; i <= size; i++ {
		p, ok := q.Dequeue()
		require.True(t, ok)
		require.Equal(t, size-i, q.Len())
		results = append(results, p)
	}
	require.Equal(t, expected, results)

	// Close the queue and assert Dequeue returns false
	q.Close()
	_, ok := q.Dequeue()
	require.False(t, ok)
}

func TestPacketQueueClose(t *testing.T) {
	q := testPacketQueue(1)
	require.Equal(t, int32(0), q.closed)

	q.Close()
	require.Equal(t, int32(1), q.closed)

	// Close the queue after it's already closed should be a no-op
	q.Close()
	require.Equal(t, int32(1), q.closed)
}
