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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3x/errors"
)

type flushManager struct {
	sync.RWMutex

	database    database
	opts        Options
	blockSize   time.Duration
	pm          persist.Manager
	flushStates map[time.Time]fileOpState
}

func newFlushManager(database database) databaseFlushManager {
	opts := database.Options()
	blockSize := opts.GetRetentionOptions().GetBlockSize()
	pm := opts.GetNewPersistManagerFn()()

	return &flushManager{
		database:    database,
		opts:        opts,
		blockSize:   blockSize,
		pm:          pm,
		flushStates: map[time.Time]fileOpState{},
	}
}

func (mgr *flushManager) HasFlushed(t time.Time) bool {
	mgr.RLock()
	defer mgr.RUnlock()

	flushState, exists := mgr.flushStates[t]
	if !exists {
		return false
	}
	return flushState.Status == fileOpSuccess
}

func (mgr *flushManager) FlushTimeStart(t time.Time) time.Time {
	retentionPeriod := mgr.opts.GetRetentionOptions().GetRetentionPeriod()
	return t.Add(-retentionPeriod).Truncate(mgr.blockSize)
}

func (mgr *flushManager) FlushTimeEnd(t time.Time) time.Time {
	bufferPast := mgr.opts.GetRetentionOptions().GetBufferPast()
	return t.Add(-bufferPast).Add(-mgr.blockSize).Truncate(mgr.blockSize)
}

func (mgr *flushManager) Flush(t time.Time) error {
	timesToFlush := mgr.flushTimes(t)
	if len(timesToFlush) == 0 {
		return nil
	}
	ctx := mgr.opts.GetContextPool().Get()
	defer ctx.Close()

	multiErr := xerrors.NewMultiError()
	for _, flushTime := range timesToFlush {
		mgr.Lock()
		if !mgr.needsFlush(flushTime) {
			continue
		}
		flushState := mgr.flushStates[flushTime]
		flushState.Status = fileOpInProgress
		mgr.flushStates[flushTime] = flushState
		mgr.Unlock()

		flushErr := mgr.flushWithTime(ctx, flushTime)

		mgr.Lock()
		flushState = mgr.flushStates[flushTime]
		if flushErr == nil {
			flushState.Status = fileOpSuccess
		} else {
			flushState.Status = fileOpFailed
			flushState.NumFailures++
			multiErr = multiErr.Add(flushErr)
		}
		mgr.flushStates[flushTime] = flushState
		mgr.Unlock()
	}
	return multiErr.FinalError()
}

// flushTimes returns a list of times we need to flush data blocks for.
func (mgr *flushManager) flushTimes(t time.Time) []time.Time {
	earliest, latest := mgr.FlushTimeStart(t), mgr.FlushTimeEnd(t)

	// NB(xichen): could preallocate slice here.
	var flushTimes []time.Time
	mgr.RLock()
	for flushTime := latest; !flushTime.Before(earliest); flushTime = flushTime.Add(-mgr.blockSize) {
		if mgr.needsFlush(flushTime) {
			flushTimes = append(flushTimes, flushTime)
		}
	}
	mgr.RUnlock()

	return flushTimes
}

// needsFlush returns true if we need to flush data for a given time.
func (mgr *flushManager) needsFlush(t time.Time) bool {
	flushState, exists := mgr.flushStates[t]
	if !exists {
		return true
	}
	return flushState.Status == fileOpFailed && flushState.NumFailures < mgr.opts.GetMaxFlushRetries()
}

func (mgr *flushManager) flushWithTime(ctx context.Context, t time.Time) error {
	multiErr := xerrors.NewMultiError()
	shards := mgr.database.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(ctx, t, mgr.pm); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v", shard.ShardNum(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}
