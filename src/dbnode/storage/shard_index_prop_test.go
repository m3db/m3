// Copyright (c) 2020 Uber Technologies, Inc.
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
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testShard = 0
)

func TestShardWarmAndColdIndexWritePropTest(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 10
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Concurrent Warm and Cold Writes Succeed", prop.ForAll(
		func(numWarmSeries, numColdSeries, numBlockOffset int) bool {
			testShardWarmAndColdIndexWrite(
				t,
				numWarmSeries,
				numColdSeries,
				numBlockOffset,
			)
			return true
		},
		gen.IntRange(10, 100).WithLabel("numWarmSeries"),
		gen.IntRange(10, 100).WithLabel("numColdSeries"),
		gen.IntRange(2, 24).WithLabel("numBlockOffset"),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}

}

type idAndTags struct {
	id   ident.ID
	tags []ident.Tag
}

func (i idAndTags) TagIterator() ident.TagIterator {
	return ident.NewTagsIterator(ident.NewTags(i.tags...))
}

func testShardWarmAndColdIndexWrite(
	t *testing.T,
	numWarmSeries int,
	numColdSeries int,
	numBlockOffset int,
) {
	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	shard, opts := testDatabaseShardWithIndex(t, metadata)
	defer func() {
		shard.Close()
		opts.RuntimeOptionsManager().Close()
	}()

	writes := []idAndTags{}
	for i := 0; i < numWarmSeries+numColdSeries; i++ {
		id := ident.StringID(fmt.Sprintf("foo.%d", i))
		writes = append(writes, idAndTags{
			id: id,
			tags: []ident.Tag{
				{
					Name:  ident.StringID("__name__"),
					Value: id,
				},
			},
		})
	}

	var (
		numRoutines    = len(writes) /* Write(s) */
		barrier        = make(chan struct{}, numRoutines)
		indexBlockSize = metadata.Options().IndexOptions().BlockSize()
		wg             sync.WaitGroup
	)

	wg.Add(numRoutines)

	doneFn := func() {
		if r := recover(); r != nil {
			assert.Fail(t, "unexpected panic: %v", r)
		}
		wg.Done()
	}

	offset := indexBlockSize * time.Duration(numBlockOffset)
	for idx, w := range writes {
		id := w.id
		tagIter := w.TagIterator()
		if idx >= numColdSeries {
			offset = time.Duration(0)
		}
		go func() {
			defer doneFn()
			<-barrier
			ctx := context.NewContext()
			now := time.Now().Add(-offset)
			_, wasWritten, err := shard.WriteTagged(
				ctx,
				id,
				tagIter,
				now,
				1.0,
				xtime.Second,
				nil,
				series.WriteOptions{},
			)
			assert.NoError(t, err)
			assert.True(t, wasWritten)
			ctx.BlockingClose()
		}()
	}

	for i := 0; i < numRoutines; i++ {
		barrier <- struct{}{}
	}

	wg.Wait()

	testSchemaHistory := prototest.NewSchemaHistory()
	testSchema := prototest.NewMessageDescriptor(testSchemaHistory)
	testSchemaDesc := namespace.GetTestSchemaDescr(testSchema)
	testNsCtx := namespace.Context{ID: metadata.ID(), Schema: testSchemaDesc}

	starts := make([]time.Time, 0)
	now := time.Now()
	blockStart := now.Add(-metadata.Options().RetentionOptions().RetentionPeriod()).Truncate(indexBlockSize)
	for blockStart.Before(now) {
		starts = append(starts, blockStart)
		blockStart = blockStart.Add(indexBlockSize)
	}
	for _, w := range writes {
		wg.Add(1)
		id := w.id
		go func() {
			defer wg.Done()
			ctx := context.NewContext()
			log.Println("fetching blocks for id:", id)
			blocks, err := shard.FetchBlocks(
				ctx,
				id,
				starts,
				testNsCtx,
			)
			require.NoError(t, err)
			require.True(t, len(blocks) > 0)
			log.Println("fetching blocks for id done:", id, "num blocks:", len(blocks))
		}()
	}
	wg.Wait()
}

func testDatabaseShardWithIndex(
	t *testing.T,
	md namespace.Metadata,
) (*dbShard, Options) {
	opts := DefaultTestOptions().SetRuntimeOptionsManager(runtime.NewOptionsManager())

	idx, err := newNamespaceIndex(md, testShardSet, opts)
	require.NoError(t, err)

	nsReaderMgr := newNamespaceReaderManager(md, tally.NoopScope, opts)
	seriesOpts := NewSeriesOptionsFromOptions(opts, defaultTestNs1Opts.RetentionOptions()).
		SetBufferBucketVersionsPool(series.NewBufferBucketVersionsPool(nil)).
		SetBufferBucketPool(series.NewBufferBucketPool(nil))
	return newDatabaseShard(md, testShard, nil, nsReaderMgr,
		&testIncreasingIndex{}, idx, true, opts, seriesOpts).(*dbShard), opts
}
