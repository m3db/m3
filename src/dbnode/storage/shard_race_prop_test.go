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
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardTickReadFnRace(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 200
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Concurrent Tick and Shard Fn doesn't panic", prop.ForAll(
		func(ids []ident.ID, tickBatchSize uint8, fn testShardReadFn) bool {
			testShardTickReadFnRace(t, ids, int(tickBatchSize), fn)
			return true
		},
		anyIDs().WithLabel("ids"),
		gen.UInt8().WithLabel("tickBatchSize").SuchThat(func(x uint8) bool { return x > 0 }),
		gen.OneConstOf(fetchBlocksMetadataV2ShardFn),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func testShardTickReadFnRace(t *testing.T, ids []ident.ID, tickBatchSize int, fn testShardReadFn) {
	shard, opts := propTestDatabaseShard(t, tickBatchSize)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	for _, id := range ids {
		addTestSeries(shard, id)
	}
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}, TickOptions{})
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		fn(shard)
		wg.Done()
	}()

	wg.Wait()
}

type testShardReadFn func(shard *dbShard)

var fetchBlocksMetadataV2ShardFn testShardReadFn = func(shard *dbShard) {
	ctx := context.NewBackground()
	start := xtime.UnixNano(0)
	end := xtime.Now()
	shard.FetchBlocksMetadataV2(ctx, start, end, 100, nil, block.FetchBlocksMetadataOptions{
		IncludeChecksums: true,
		IncludeLastRead:  true,
		IncludeSizes:     true,
	})
	ctx.BlockingClose()
}

func propTestDatabaseShard(t *testing.T, tickBatchSize int) (*dbShard, Options) {
	opts := DefaultTestOptions().SetRuntimeOptionsManager(runtime.NewOptionsManager())
	shard := testDatabaseShard(t, opts)
	// This sleep duration needs to be at the microsecond level because tests
	// can have a high number of iterations, using a high number of series in
	// combination with a small batch size, causing frequent timeouts during
	// execution.
	shard.currRuntimeOptions.tickSleepPerSeries = time.Microsecond
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = tickBatchSize
	return shard, opts
}

func anyIDs() gopter.Gen {
	return gen.IntRange(0, 20).
		Map(func(n int) []ident.ID {
			ids := make([]ident.ID, 0, n)
			for i := 0; i < n; i++ {
				ids = append(ids, ident.StringID(fmt.Sprintf("foo.%d", i)))
			}
			return ids
		})
}

func TestShardTickWriteRace(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 200
	parameters.MaxSize = 10
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Concurrent Tick and Write doesn't deadlock", prop.ForAll(
		func(tickBatchSize, numSeries int) bool {
			testShardTickWriteRace(t, tickBatchSize, numSeries)
			return true
		},
		gen.IntRange(1, 100).WithLabel("tickBatchSize"),
		gen.IntRange(1, 100).WithLabel("numSeries"),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func testShardTickWriteRace(t *testing.T, tickBatchSize, numSeries int) {
	shard, opts := propTestDatabaseShard(t, tickBatchSize)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	ids := []ident.ID{}
	for i := 0; i < numSeries; i++ {
		ids = append(ids, ident.StringID(fmt.Sprintf("foo.%d", i)))
	}

	var (
		numRoutines = 1 + /* Fetch */ +1 /* Tick */ + len(ids) /* Write(s) */
		barrier     = make(chan struct{}, numRoutines)
		wg          sync.WaitGroup
	)

	wg.Add(numRoutines)

	doneFn := func() {
		if r := recover(); r != nil {
			assert.Fail(t, "unexpected panic: %v", r)
		}
		wg.Done()
	}

	for _, id := range ids {
		id := id
		go func() {
			defer doneFn()
			<-barrier
			ctx := context.NewBackground()
			now := xtime.Now()
			seriesWrite, err := shard.Write(ctx, id, now, 1.0, xtime.Second, nil, series.WriteOptions{})
			assert.NoError(t, err)
			assert.True(t, seriesWrite.WasWritten)
			ctx.BlockingClose()
		}()
	}

	go func() {
		defer doneFn()
		<-barrier
		fetchBlocksMetadataV2ShardFn(shard)
	}()

	go func() {
		defer doneFn()
		<-barrier
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}, TickOptions{})
		assert.NoError(t, err)
	}()

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()
}

func TestShardTickBootstrapWriteRace(t *testing.T) {
	shard, opts := propTestDatabaseShard(t, 10)
	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "unexpected panic: %v", r)
		}
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	// distribute ids into 3 categories
	// (1) existing in the shard prior to bootstrap (for w/e reason)
	// (2) actively being written to by Write()
	// (3) inserted via Bootstrap()
	// further, we ensure there's pairwise overlaps between each pair of categories.

	// total ids = 30, splitting id space into following
	// (1) - existingIDs - [0, 20)
	// (2) - writeIDs - [10, 30)
	// (3) - bootstrapIDs - [0, 10) U [] [20, 30)

	var writeIDs []ident.ID
	bootstrapResult := result.NewMap(result.MapOptions{})

	for i := 0; i < 30; i++ {
		id := ident.StringID(fmt.Sprintf("foo.%d", i))
		// existing ids
		if i < 20 {
			addTestSeriesWithCount(shard, id, 0)
		}
		// write ids
		if i >= 10 {
			writeIDs = append(writeIDs, id)
		}
		// botstrap ids
		if i < 10 || i >= 20 {
			bootstrapResult.Set(id, result.DatabaseSeriesBlocks{
				ID:     id,
				Tags:   ident.NewTags(),
				Blocks: block.NewDatabaseSeriesBlocks(3),
			})
		}
	}

	var (
		numRoutines = 1 + /* Bootstrap */ +1 /* Tick */ + len(writeIDs) /* Write(s) */
		barrier     = make(chan struct{}, numRoutines)
		wg          sync.WaitGroup
	)

	wg.Add(numRoutines)

	doneFn := func() {
		if r := recover(); r != nil {
			assert.Fail(t, "unexpected panic: %v", r)
		}
		wg.Done()
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	assert.NoError(t, shard.Bootstrap(ctx, namespace.Context{ID: ident.StringID("foo")}))
	for _, id := range writeIDs {
		id := id
		go func() {
			defer doneFn()
			<-barrier
			ctx := context.NewBackground()
			now := xtime.Now()
			seriesWrite, err := shard.Write(ctx, id, now, 1.0, xtime.Second, nil, series.WriteOptions{})
			assert.NoError(t, err)
			assert.True(t, seriesWrite.WasWritten)
			ctx.BlockingClose()
		}()
	}

	go func() {
		defer doneFn()
		<-barrier
		err := shard.LoadBlocks(bootstrapResult)
		assert.NoError(t, err)
	}()

	go func() {
		defer doneFn()
		<-barrier
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}, TickOptions{})
		assert.NoError(t, err)
	}()

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()
}
