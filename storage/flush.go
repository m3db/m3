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

package storage

import (
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
)

type flushStatus byte

const (
	flushNotStarted flushStatus = iota
	flushInProgress
	flushSuccess
	flushFailed
)

type flushState struct {
	Status      flushStatus
	NumFailures int
}

type flushManager struct {
	sync.RWMutex

	opts           Options                // storage options
	database       database               // storage database
	pm             persist.PersistManager // persistence manager
	fs             flushStatus
	flushAttempted map[time.Time]flushState
	flushTimes     []time.Time
}

func newFlushManager(database database) databaseFlushManager {
	opts := database.Options()
	pm := opts.GetNewPersistManagerFn()()
	rops := opts.GetRetentionOptions()
	numSlots := int(math.Ceil(float64(rops.GetRetentionPeriod()) / float64(rops.GetBlockSize())))

	return &flushManager{
		database:       database,
		pm:             pm,
		opts:           opts,
		fs:             flushNotStarted,
		flushAttempted: make(map[time.Time]flushState),
		flushTimes:     make([]time.Time, 0, numSlots),
	}
}

func (fm *flushManager) NeedsFlush(t time.Time) bool {
	// If we haven't bootstrapped yet, don't flush.
	if !fm.database.IsBootstrapped() {
		return false
	}

	firstBlockStart := fm.getFirstBlockStart(t)
	fm.RLock()
	defer fm.RUnlock()
	// If we are in the middle of flushing data, don't flush.
	if fm.fs == flushInProgress {
		return false
	}
	// If we have already tried flushing for this block start time, don't try again.
	if _, exists := fm.flushAttempted[firstBlockStart]; exists {
		return false
	}
	return true
}

func (fm *flushManager) getFirstBlockStart(t time.Time) time.Time {
	rops := fm.opts.GetRetentionOptions()
	bufferPast := rops.GetBufferPast()
	blockSize := rops.GetBlockSize()
	return t.Add(-bufferPast).Add(-blockSize).Truncate(blockSize)
}

func (fm *flushManager) Flush(t time.Time, async bool) {
	timesToFlush := fm.getTimesToFlush(t)
	if len(timesToFlush) == 0 {
		return
	}

	fm.Lock()
	if fm.fs == flushInProgress {
		fm.Unlock()
		return
	}
	fm.fs = flushInProgress
	fm.Unlock()

	flushFn := func() {
		ctx := fm.opts.GetContextPool().Get()
		defer ctx.Close()

		for _, flushTime := range timesToFlush {
			success := fm.flushWithTime(ctx, flushTime)
			fm.Lock()
			flushState := fm.flushAttempted[flushTime]
			if success {
				flushState.Status = flushSuccess
			} else {
				flushState.Status = flushFailed
				flushState.NumFailures++
			}
			fm.flushAttempted[flushTime] = flushState
			fm.Unlock()
		}

		fm.Lock()
		fm.fs = flushNotStarted
		fm.Unlock()
	}

	if !async {
		flushFn()
	} else {
		go flushFn()
	}
}

func (fm *flushManager) getTimesToFlush(t time.Time) []time.Time {
	rops := fm.opts.GetRetentionOptions()
	blockSize := rops.GetBlockSize()
	maxFlushRetries := fm.opts.GetMaxFlushRetries()
	firstBlockStart := fm.getFirstBlockStart(t)
	earliestTime := t.Add(-1 * rops.GetRetentionPeriod())

	fm.Lock()
	defer fm.Unlock()
	fm.flushTimes = fm.flushTimes[:0]
	for flushTime := firstBlockStart; !flushTime.Before(earliestTime); flushTime = flushTime.Add(-blockSize) {
		if flushState, exists := fm.flushAttempted[flushTime]; !exists || (flushState.Status == flushFailed && flushState.NumFailures < maxFlushRetries) {
			flushState.Status = flushInProgress
			fm.flushTimes = append(fm.flushTimes, flushTime)
			fm.flushAttempted[flushTime] = flushState
		}
	}
	return fm.flushTimes
}

func (fm *flushManager) flushWithTime(ctx context.Context, t time.Time) bool {
	allShardsSucceeded := true
	log := fm.opts.GetInstrumentOptions().GetLogger()
	shards := fm.database.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(ctx, t, fm.pm); err != nil {
			log.Errorf("shard %d failed to flush data: %v", shard.ShardNum(), err)
			allShardsSucceeded = false
		}
	}
	return allShardsSucceeded
}
