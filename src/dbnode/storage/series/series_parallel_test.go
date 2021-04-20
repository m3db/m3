// +build big
//
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

package series

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// TestSeriesWriteReadParallel is a regression test that was added to capture
// panics that might arise when many parallel writes and reads are happening
// at the same time.
func TestSeriesWriteReadParallel(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	// Assume all data has not been written yet.
	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	var (
		numWorkers        = 100
		numStepsPerWorker = numWorkers * 100
		opts              = newSeriesTestOptions()
		start             = time.Now()
		series            = NewDatabaseSeries(DatabaseSeriesOptions{
			ID:             ident.StringID("foo"),
			UniqueIndex:    1,
			BlockRetriever: blockRetriever,
			Options:        opts,
		}).(*dbSeries)
		dbBlock = block.NewDatabaseBlock(time.Time{}, time.Hour*2,
			ts.Segment{}, block.NewOptions(), namespace.Context{})
	)

	err := series.LoadBlock(dbBlock, WarmWrite)
	require.NoError(t, err)

	ctx := context.NewBackground()
	defer ctx.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 0; i < numStepsPerWorker; i++ {
			wasWritten, _, err := series.Write(ctx, time.Now(), float64(i),
				xtime.Nanosecond, nil, WriteOptions{})
			if err != nil {
				panic(err)
			}
			if !wasWritten {
				panic("write failed")
			}
		}
		wg.Done()
	}()

	// Outer loop so that reads are competing with other reads, not just writes.
	for j := 0; j < numWorkers; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < numStepsPerWorker; i++ {
				now := time.Now()
				_, err := series.ReadEncoded(ctx, start.Add(-time.Minute),
					now.Add(time.Minute), namespace.Context{})
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
