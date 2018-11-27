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

	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
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
		shard.Tick(context.NewNoOpCanncellable(), time.Now())
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
	ctx := context.NewContext()
	start := time.Time{}
	end := time.Now()
	shard.FetchBlocksMetadataV2(ctx, start, end, 100, nil, block.FetchBlocksMetadataOptions{
		IncludeChecksums: true,
		IncludeLastRead:  true,
		IncludeSizes:     true,
	})
	ctx.BlockingClose()
}

func propTestDatabaseShard(t *testing.T, tickBatchSize int) (*dbShard, Options) {
	opts := testDatabaseOptions().SetRuntimeOptionsManager(runtime.NewOptionsManager())
	shard := testDatabaseShard(t, opts)
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
	shard, opts := propTestDatabaseShard(t, 10)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	ids := []ident.ID{}
	for i := 0; i < 1; i++ {
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
			ctx := context.NewContext()
			now := time.Now()
			_, err := shard.Write(ctx, id, now, series.WarmWrite, 1.0, xtime.Second, nil)
			assert.NoError(t, err)
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
		shard.Tick(context.NewNoOpCanncellable(), time.Now())
	}()

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()
}
