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

func (m *flushManager) HasFlushed(t time.Time) bool {
	m.RLock()
	defer m.RUnlock()

	flushState, exists := m.flushStates[t]
	if !exists {
		return false
	}
	return flushState.Status == fileOpSuccess
}

func (m *flushManager) FlushTimeStart(t time.Time) time.Time {
	retentionPeriod := m.opts.GetRetentionOptions().GetRetentionPeriod()
	return t.Add(-retentionPeriod).Truncate(m.blockSize)
}

func (m *flushManager) FlushTimeEnd(t time.Time) time.Time {
	bufferPast := m.opts.GetRetentionOptions().GetBufferPast()
	return t.Add(-bufferPast).Add(-m.blockSize).Truncate(m.blockSize)
}

func (m *flushManager) Flush(t time.Time) error {
	timesToFlush := m.flushTimes(t)
	if len(timesToFlush) == 0 {
		return nil
	}
	ctx := m.opts.GetContextPool().Get()
	defer ctx.Close()

	multiErr := xerrors.NewMultiError()
	for _, flushTime := range timesToFlush {
		m.Lock()
		if !m.needsFlushWithLock(flushTime) {
			m.Unlock()
			continue
		}
		flushState := m.flushStates[flushTime]
		flushState.Status = fileOpInProgress
		m.flushStates[flushTime] = flushState
		m.Unlock()

		flushErr := m.flushWithTime(ctx, flushTime)

		m.Lock()
		flushState = m.flushStates[flushTime]
		if flushErr == nil {
			flushState.Status = fileOpSuccess
		} else {
			flushState.Status = fileOpFailed
			flushState.NumFailures++
			multiErr = multiErr.Add(flushErr)
		}
		m.flushStates[flushTime] = flushState
		m.Unlock()
	}
	return multiErr.FinalError()
}

// flushTimes returns a list of times we need to flush data blocks for.
func (m *flushManager) flushTimes(t time.Time) []time.Time {
	earliest, latest := m.FlushTimeStart(t), m.FlushTimeEnd(t)

	// NB(xichen): could preallocate slice here.
	var flushTimes []time.Time
	m.RLock()
	for flushTime := latest; !flushTime.Before(earliest); flushTime = flushTime.Add(-m.blockSize) {
		if m.needsFlushWithLock(flushTime) {
			flushTimes = append(flushTimes, flushTime)
		}
	}
	m.RUnlock()

	return flushTimes
}

// needsFlushWithLock returns true if we need to flush data for a given time.
func (m *flushManager) needsFlushWithLock(t time.Time) bool {
	flushState, exists := m.flushStates[t]
	if !exists {
		return true
	}
	return flushState.Status == fileOpFailed && flushState.NumFailures < m.opts.GetMaxFlushRetries()
}

func (m *flushManager) flushWithTime(ctx context.Context, t time.Time) error {
	multiErr := xerrors.NewMultiError()
	shards := m.database.getOwnedShards()
	for _, shard := range shards {
		// NB(xichen): we still want to proceed if a shard fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := shard.Flush(ctx, t, m.pm); err != nil {
			detailedErr := fmt.Errorf("shard %d failed to flush data: %v", shard.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}
