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

package storage

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	defaultTestOptionsOnce sync.Once
	defaultTestOptions     Options
)

// DefaultTestOptions provides a single set of test storage options
// we save considerable memory by doing this avoiding creating
// default pools several times.
func DefaultTestOptions() Options {
	defaultTestOptionsOnce.Do(func() {
		opts := newOptions(pool.NewObjectPoolOptions().
			SetSize(16))

		// Use a no-op options manager to avoid spinning up a goroutine to listen
		// for updates, which causes problems with leaktest in individual test
		// executions
		runtimeOptionsMgr := runtime.NewNoOpOptionsManager(
			runtime.NewOptions())

		blockLeaseManager := &block.NoopLeaseManager{}
		fsOpts := fs.NewOptions().
			SetRuntimeOptionsManager(runtimeOptionsMgr)
		pm, err := fs.NewPersistManager(fsOpts)
		if err != nil {
			panic(err)
		}

		plCache, err := index.NewPostingsListCache(10, index.PostingsListCacheOptions{
			InstrumentOptions: opts.InstrumentOptions(),
		})
		if err != nil {
			panic(err)
		}
		plStop := plCache.Start()
		defer plStop()

		indexOpts := opts.IndexOptions().
			SetPostingsListCache(plCache)

		defaultTestOptions = opts.
			SetIndexOptions(indexOpts).
			SetSeriesCachePolicy(series.CacheAll).
			SetPersistManager(pm).
			SetRepairEnabled(false).
			SetCommitLogOptions(
				opts.CommitLogOptions().SetFilesystemOptions(fsOpts)).
			SetBlockLeaseManager(blockLeaseManager)
	})

	// Needs a unique index claims manager each time as it tracks volume indices via in mem claims that
	// should be different per test.
	fs.ResetIndexClaimsManagersUnsafe()
	fsOpts := defaultTestOptions.CommitLogOptions().FilesystemOptions()
	icm, err := fs.NewIndexClaimsManager(fsOpts)
	if err != nil {
		panic(err)
	}

	return defaultTestOptions.SetIndexClaimsManager(icm)
}

// numIntervals returns the number of intervals between [start, end] for a given
// windowSize.
// NB: edge conditions:
// 	- returns 0 if window <= 0 ;
//  - returns 0 if end is before start;
//  - returns 1 if end == start
func numIntervals(startInclusive, endInclusive xtime.UnixNano, window time.Duration) int {
	if window <= 0 || endInclusive.Before(startInclusive) {
		return 0
	}

	return 1 + int((endInclusive.Sub(startInclusive))/window)
}

// timesInRange returns the points between [start, end] windowSize apart, starting at `end`, in
// reverse chronological order.
// NB: returns empty slice if window <= 0 or end is before start
func timesInRange(startInclusive, endInclusive xtime.UnixNano, windowSize time.Duration) []xtime.UnixNano {
	ni := numIntervals(startInclusive, endInclusive, windowSize)
	if ni == 0 {
		return nil
	}
	times := make([]xtime.UnixNano, 0, ni)
	for t := endInclusive; !t.Before(startInclusive); t = t.Add(-windowSize) {
		times = append(times, t)
	}
	return times
}

// filterTimes returns the values in the slice `times` which satisfy
// the provided predicate.
func filterTimes(times []xtime.UnixNano, predicate func(t xtime.UnixNano) bool) []xtime.UnixNano {
	filtered := make([]xtime.UnixNano, 0, len(times))
	for _, t := range times {
		if predicate(t) {
			filtered = append(filtered, t)
		}
	}

	return filtered
}
