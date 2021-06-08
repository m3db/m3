// +build big
//
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

package storage

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestShardConcurrentForEaches(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 200
	parameters.MaxSize = 40
	parameters.MaxDiscardRatio = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property(`ForEachShardEntry does not change the shard entries ref count`, prop.ForAll(
		func(entries []shardEntryState, workFns []dbShardEntryBatchWorkFn) (bool, error) {
			result := testShardConcurrentForEach(t, entries, workFns)
			if result.Status == gopter.PropTrue {
				return true, nil
			}
			return false, result.Error
		},
		genShardEntryStates(),
		genBatchWorkFns(),
	))
	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestShardConcurrentForEachesAndTick(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 200
	parameters.MaxSize = 40
	parameters.MaxDiscardRatio = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property(`Concurrent ForEachShardEntry and Tick does not panic`, prop.ForAll(
		func(entries []shardEntryState, workFns []dbShardEntryBatchWorkFn) bool {
			testShardConcurrentForEachTick(t, entries, workFns)
			return true
		},
		genShardEntryStates(),
		genBatchWorkFns(),
	))
	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func testShardConcurrentForEachTick(
	t *testing.T,
	entries []shardEntryState,
	workFns []dbShardEntryBatchWorkFn,
) {
	shard, opts := propTestDatabaseShard(t, 10)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	for _, entry := range entries {
		addTestSeriesWithCount(shard, entry.id, entry.refCount)
	}

	require.NoError(t, shardEntriesAreEqual(shard, entries))

	var (
		numRoutines = len(workFns) + 1
		barrier     = make(chan struct{}, numRoutines)
		wg          sync.WaitGroup
	)

	wg.Add(numRoutines)

	for _, fn := range workFns {
		fn := fn
		go func() {
			<-barrier
			shard.forEachShardEntryBatch(fn)
			wg.Done()
		}()
	}

	go func() {
		<-barrier
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{})
		require.NoError(t, err)
		wg.Done()
	}()

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()
}

func testShardConcurrentForEach(
	t *testing.T,
	entries []shardEntryState,
	workFns []dbShardEntryBatchWorkFn,
) gopter.PropResult {
	shard, opts := propTestDatabaseShard(t, 10)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	for _, entry := range entries {
		addTestSeriesWithCount(shard, entry.id, entry.refCount)
	}

	require.NoError(t, shardEntriesAreEqual(shard, entries))

	var (
		numRoutines = len(workFns)
		barrier     = make(chan struct{}, numRoutines)
		wg          sync.WaitGroup
	)

	wg.Add(numRoutines)

	for _, fn := range workFns {
		fn := fn
		go func() {
			<-barrier
			shard.forEachShardEntryBatch(fn)
			wg.Done()
		}()
	}

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()

	if err := shardEntriesAreEqual(shard, entries); err != nil {
		return gopter.PropResult{
			Status: gopter.PropError,
			Error:  err,
		}
	}

	return gopter.PropResult{
		Status: gopter.PropTrue,
	}
}

func shardEntriesAreEqual(shard *dbShard, expectedEntries []shardEntryState) error {
	shard.Lock()
	defer shard.Unlock()

	if len(expectedEntries) == 0 && shard.list.Front() == nil {
		return nil
	}

	elem := shard.list.Front()
	for idx, expectedEntry := range expectedEntries {
		if elem == nil {
			return fmt.Errorf("expected to have %d idx, but did not see anything", idx)
		}
		nextElem := elem.Next()
		entry := elem.Value.(*lookup.Entry)
		if !entry.Series.ID().Equal(expectedEntry.id) {
			return fmt.Errorf("expected id: %s at %d, observed: %s",
				expectedEntry.id.String(), idx, entry.Series.ID().String())
		}
		if entry.ReaderWriterCount() != expectedEntry.refCount {
			return fmt.Errorf("expected id: %s at %d to have ref count %d, observed: %d",
				entry.Series.ID().String(), idx, expectedEntry.refCount, entry.ReaderWriterCount())
		}
		elem = nextElem
	}

	if elem != nil {
		return fmt.Errorf("expected to have see %d entries, but observed more", len(expectedEntries))
	}

	return nil
}

type shardEntryState struct {
	id       ident.ID
	refCount int32
}

func genShardEntryStates() gopter.Gen {
	return gen.SliceOf(genShardEntryState())
}

func genShardEntryState() gopter.Gen {
	return gopter.CombineGens(gen.UInt(), gen.UInt16()).
		Map(
			func(values []interface{}) shardEntryState {
				return shardEntryState{
					id:       ident.StringID(fmt.Sprintf("foo%d", values[0].(uint))),
					refCount: int32(values[1].(uint16)),
				}
			},
		)
}

func genBatchWorkFns() gopter.Gen {
	return gen.SliceOf(genBatchWorkFn())
}

func genBatchWorkFn() gopter.Gen {
	return gen.UInt8().
		Map(func(n uint8) dbShardEntryBatchWorkFn {
			i := uint8(0)
			return func([]*lookup.Entry) bool {
				i++
				return i < n
			}
		})
}
