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

package cache

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testForExistingID = metadata.StagedMetadatas{
		metadata.StagedMetadata{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(20*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
						},
					},
				},
			},
		},
		metadata.StagedMetadata{
			CutoverNanos: 0,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
				},
			},
		},
	}
	testForNewRollupIDs = []rules.IDWithMetadatas{
		{
			ID:        []byte("rID1"),
			Metadatas: metadata.DefaultStagedMetadatas,
		},
		{
			ID: []byte("rID2"),
			Metadatas: metadata.StagedMetadatas{
				metadata.StagedMetadata{
					CutoverNanos: 0,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(20*time.Second, xtime.Second, 6*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
									policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
								},
							},
						},
					},
				},
				metadata.StagedMetadata{
					CutoverNanos: 0,
					Tombstoned:   true,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}
	testValidResults = []rules.MatchResult{
		rules.NewMatchResult(0, math.MaxInt64, nil, nil, true),
		rules.NewMatchResult(0, math.MaxInt64, testForExistingID, testForNewRollupIDs, true),
	}
	testExpiredResults = []rules.MatchResult{
		rules.NewMatchResult(0, 0, nil, nil, true),
		rules.NewMatchResult(0, 0, testForExistingID, testForNewRollupIDs, true),
	}
)

func TestElementShouldExpire(t *testing.T) {
	now := time.Unix(0, 1234)
	e := &element{}
	for _, input := range []struct {
		expiryNanos int64
		expected    bool
	}{
		{expiryNanos: 1233, expected: true},
		{expiryNanos: 1234, expected: true},
		{expiryNanos: 1235, expected: false},
	} {
		e.expiryNanos = input.expiryNanos
		require.Equal(t, input.expected, e.ShouldPromote(now))
	}
}

func TestListPushFront(t *testing.T) {
	var (
		l      list
		iter   = 10
		inputs = make([]testValue, iter)
	)
	for i := 0; i < iter; i++ {
		namespace := []byte(fmt.Sprintf("namespace%d", i))
		id := []byte(fmt.Sprintf("foo%d", i))
		result := testValidResults[i%2]
		inputs[iter-i-1] = testValue{namespace: namespace, id: id, result: result}
		l.PushFront(&element{
			namespace: namespace,
			id:        id,
			result:    result,
		})
	}

	// Pushing front a nil pointer is a no-op.
	l.PushFront(nil)

	// Pushing front a deleted element is a no-op.
	l.PushFront(&element{
		namespace: []byte("deletedNs"),
		result:    testValidResults[0],
		deleted:   true,
	})

	validateList(t, &l, inputs)
}

func TestListPushBack(t *testing.T) {
	var (
		l      list
		iter   = 10
		inputs = make([]testValue, iter)
	)
	for i := 0; i < iter; i++ {
		namespace := []byte(fmt.Sprintf("namespace%d", i))
		id := []byte(fmt.Sprintf("foo%d", i))
		result := testValidResults[i%2]
		inputs[i] = testValue{namespace: namespace, id: id, result: result}
		l.PushBack(&element{
			namespace: namespace,
			id:        id,
			result:    result,
		})
	}

	// Pushing back a nil pointer is a no-op.
	l.PushBack(nil)

	// Pushing back a deleted element is a no-op.
	l.PushBack(&element{
		namespace: []byte("deletedNs"),
		result:    testValidResults[0],
		deleted:   true,
	})

	validateList(t, &l, inputs)
}

func TestListRemove(t *testing.T) {
	var (
		l      list
		iter   = 10
		inputs = make([]testValue, iter)
	)
	for i := 0; i < iter; i++ {
		namespace := []byte(fmt.Sprintf("namespace%d", i))
		id := []byte(fmt.Sprintf("foo%d", i))
		result := testValidResults[i%2]
		inputs[i] = testValue{namespace: namespace, id: id, result: result}
		l.PushBack(&element{
			namespace: namespace,
			id:        id,
			result:    result,
		})
	}

	// Removing a nil pointer is no-op.
	l.Remove(nil)

	// Removing a deleted element is a no-op.
	l.Remove(&element{
		namespace: []byte("deletedNs"),
		result:    testValidResults[0],
		deleted:   true,
	})

	for i := 0; i < iter; i++ {
		elem := l.Front()
		l.Remove(elem)
		require.Nil(t, elem.prev)
		require.Nil(t, elem.next)
		validateList(t, &l, inputs[i+1:])
	}
}

func TestListMoveToFront(t *testing.T) {
	var (
		l      list
		iter   = 10
		inputs = make([]testValue, iter)
	)
	for i := 0; i < iter; i++ {
		namespace := []byte(fmt.Sprintf("namespace%d", i))
		id := []byte(fmt.Sprintf("foo%d", i))
		result := testValidResults[i%2]
		inputs[i] = testValue{namespace: namespace, id: id, result: result}
		l.PushBack(&element{
			namespace: namespace,
			id:        id,
			result:    result,
		})
	}

	// Moving a nil pointer to front is a no-op.
	l.MoveToFront(nil)

	// Moving a deleted element to front is a no-op.
	l.MoveToFront(&element{
		namespace: []byte("deletedNs"),
		result:    testValidResults[0],
		deleted:   true,
	})

	// Starting from the back, moving elements to front one at a time.
	var prev, curr, last *element
	for {
		if curr == last && curr != nil {
			break
		}
		if last == nil {
			last = l.Back()
			curr = last
		}
		prev = curr.prev
		l.MoveToFront(curr)
		require.Equal(t, l.head, curr)
		require.Nil(t, curr.prev)
		curr = prev
	}
	validateList(t, &l, inputs)
}

type testValue struct {
	namespace []byte
	id        []byte
	result    rules.MatchResult
}

func (t testValue) ID() id.ID {
	return namespace.NewTestID(string(t.id), string(t.namespace))
}

func validateList(t *testing.T, l *list, expected []testValue) {
	require.Equal(t, len(expected), l.Len())
	i := 0
	for elem := l.Front(); elem != nil; elem = elem.next {
		require.Equal(t, expected[i].namespace, elem.namespace)
		require.Equal(t, expected[i].id, elem.id)
		require.Equal(t, expected[i].result, elem.result)
		i++
	}
	if len(expected) == 0 {
		require.Nil(t, l.head)
		require.Nil(t, l.tail)
	} else {
		require.Equal(t, l.Front(), l.head)
		require.Nil(t, l.head.prev)
		require.Equal(t, l.Back(), l.tail)
		require.Nil(t, l.tail.next)
	}
}
