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

package aggregator

import (
	"fmt"
	"testing"
	"time"

	schema "github.com/m3db/m3/src/aggregator/generated/proto/flush"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testFlushTimesKeyFmt = "test/%d/flush"
)

var (
	testFlushTimesKey   = fmt.Sprintf(testFlushTimesKeyFmt, testShardSetID)
	testFlushTimesProto = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					int64(time.Second): 1000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 700,
						},
					},
				},
				TimedByResolution: map[int64]int64{
					int64(time.Second): 500,
				},
			},
			1: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					int64(time.Minute): 2000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 2500,
							3: 3500,
						},
					},
				},
				TimedByResolution: map[int64]int64{
					int64(time.Second): 1500,
				},
			},
		},
	}
)

func TestFlushTimesManagerReset(t *testing.T) {
	mgr, _ := testFlushTimesManager()

	// Reseting an unopened manager is a no op.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())

	// Opening a closed manager causes an error.
	require.Error(t, mgr.Open(testShardSetID))

	// Reseting the manager allows the manager to be reopened.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())

	// Resetting an open manager causes an error.
	mgr.state = flushTimesManagerOpen
	require.Equal(t, errFlushTimesManagerOpen, mgr.Reset())
}

func TestFlushTimesManagerOpenAlreadyOpen(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	mgr.state = flushTimesManagerOpen
	require.Equal(t, errFlushTimesManagerAlreadyOpenOrClosed, mgr.Open(testShardSetID))
}

func TestFlushTimesManagerOpenSuccess(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))
}

func TestFlushTimesManagerGetClosed(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	_, err := mgr.Get()
	require.Equal(t, errFlushTimesManagerNotOpenOrClosed, err)
}

func TestFlushTimesManagerGetSuccess(t *testing.T) {
	mgr, store := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))

	// Update the flush times and wait for the change to propagate.
	_, err := store.Set(testFlushTimesKey, testFlushTimesProto)
	require.NoError(t, err)
	for {
		if mgr.flushTimesWatchable.Get() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	res, err := mgr.Get()
	require.NoError(t, err)
	require.Equal(t, res, testFlushTimesProto)
}

func TestFlushTimesManagerWatchClosed(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	_, err := mgr.Watch()
	require.Equal(t, errFlushTimesManagerNotOpenOrClosed, err)
}

func TestFlushTimesManagerWatchSuccess(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))

	watch, err := mgr.Watch()
	require.NoError(t, err)

	select {
	case <-watch.C():
		require.Fail(t, "unexpected watch notification")
	default:
	}

	mgr.flushTimesWatchable.Update(12345)
	<-watch.C()
	require.Equal(t, 12345, watch.Get().(int))
}

func TestFlushTimesManagerStoreAsyncClosed(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	require.Equal(t, errFlushTimesManagerNotOpenOrClosed, mgr.StoreAsync(testFlushTimesProto))
}

func TestFlushTimesManagerStoreAsyncSuccess(t *testing.T) {
	mgr, store := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))

	// Store flush times and wait for change to propagate.
	require.NoError(t, mgr.StoreAsync(testFlushTimesProto))
	for {
		value, err := store.Get(testFlushTimesKey)
		if value != nil && err == nil {
			var res schema.ShardSetFlushTimes
			require.NoError(t, value.Unmarshal(&res))
			require.Equal(t, *testFlushTimesProto, res)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFlushTimesManagerStoreSyncSuccess(t *testing.T) {
	mgr, store := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))

	// Store flush times and wait for change to propagate.
	require.NoError(t, mgr.StoreSync(testFlushTimesProto))
	value, err := store.Get(testFlushTimesKey)
	require.NoError(t, err)
	require.NotNil(t, value)
	var res schema.ShardSetFlushTimes
	require.NoError(t, value.Unmarshal(&res))
	require.Equal(t, *testFlushTimesProto, res)
	return
}

func TestFlushTimesManagerCloseClosed(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	require.Equal(t, errFlushTimesManagerNotOpenOrClosed, mgr.Close())
}

func TestFlushTimesManagerCloseSuccess(t *testing.T) {
	mgr, _ := testFlushTimesManager()
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())
}

func TestFlushTimesCheckerHasFlushed(t *testing.T) {
	checker := newFlushTimesChecker(tally.NoopScope)
	inputs := []struct {
		shardID     uint32
		targetNanos int64
		flushTimes  *schema.ShardSetFlushTimes
		expected    bool
	}{
		{shardID: 0, targetNanos: 700, flushTimes: testFlushTimesProto, expected: false},
		{shardID: 0, targetNanos: 300, flushTimes: testFlushTimesProto, expected: true},
		{shardID: 1, targetNanos: 2000, flushTimes: testFlushTimesProto, expected: false},
		{shardID: 1, targetNanos: 1200, flushTimes: testFlushTimesProto, expected: true},
		{shardID: 0, targetNanos: 0, flushTimes: nil, expected: false},
		{shardID: 0, targetNanos: 1000, flushTimes: testFlushTimesProto, expected: false},
		{shardID: 1, targetNanos: 3000, flushTimes: testFlushTimesProto, expected: false},
		{shardID: 2, targetNanos: 0, flushTimes: testFlushTimesProto, expected: false},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, checker.HasFlushed(input.shardID, input.targetNanos, input.flushTimes))
	}
}

func testFlushTimesManager() (*flushTimesManager, kv.Store) {
	store := mem.NewStore()
	opts := NewFlushTimesManagerOptions().
		SetFlushTimesKeyFmt(testFlushTimesKeyFmt).
		SetFlushTimesStore(store)
	return NewFlushTimesManager(opts).(*flushTimesManager), store
}
