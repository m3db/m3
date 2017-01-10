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

package packet

import (
	"fmt"
	"testing"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func testQueue(size int) *Queue {
	return NewQueue(size, clock.NewOptions(), instrument.NewOptions())
}

func TestQueueEnqueue(t *testing.T) {
	size := 10
	q := testQueue(size)

	// Enqueue up to the queue size
	for i := 1; i <= size; i++ {
		q.Enqueue(Packet{})
		require.Equal(t, i, q.Len())
	}

	// Enqueue one more and assert the packet gets dropped
	q.Enqueue(Packet{})
	require.Equal(t, size, q.Len())

	// Enqueuing to a closed queue should result in an error
	q.Close()
	require.Equal(t, errQueueClosed, q.Enqueue(Packet{}))
}

func TestQueueDequeue(t *testing.T) {
	var (
		expected []Packet
		results  []Packet
		size     = 10
		q        = testQueue(size)
	)
	for i := 1; i <= size; i++ {
		p := Packet{
			Metric: unaggregated.MetricUnion{
				Type:       unaggregated.CounterType,
				ID:         []byte(fmt.Sprintf("%d", i)),
				CounterVal: int64(i),
			},
			Policies: policy.DefaultVersionedPolicies,
		}
		expected = append(expected, p)
		q.Enqueue(p)
	}

	// Dequeue until the queue is empty
	for i := 1; i <= size; i++ {
		p, err := q.Dequeue()
		require.NoError(t, err)
		require.Equal(t, size-i, q.Len())
		results = append(results, p)
	}
	require.Equal(t, expected, results)

	// Close the queue and assert Dequeue returns false
	q.Close()
	_, err := q.Dequeue()
	require.Equal(t, errQueueClosed, err)
}

func TestQueueClose(t *testing.T) {
	q := testQueue(1)
	require.Equal(t, int32(0), q.closed)

	q.Close()
	require.Equal(t, int32(1), q.closed)

	// Close the queue after it's already closed should be a no-op
	q.Close()
	require.Equal(t, int32(1), q.closed)
}
