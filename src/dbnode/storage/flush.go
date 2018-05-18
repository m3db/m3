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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/retention"
	xerrors "github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

var (
	errFlushOperationsInProgress = errors.New("flush operations already in progress")
)

type flushManagerState int

const (
	flushManagerIdle flushManagerState = iota
	// flushManagerNotIdle is used to protect the flush manager from concurrent use
	// when we haven't begun either a flush or snapshot.
	flushManagerNotIdle
	flushManagerFlushInProgress
	flushManagerSnapshotInProgress
	flushManagerIndexFlushInProgress
)

type flushManager struct {
	sync.RWMutex

	database database
	opts     Options
	pm       persist.Manager
	// isFlushingOrSnapshotting is used to protect the flush manager against
	// concurrent use, while flushInProgress and snapshotInProgress are more
	// granular and are used for emitting granular gauges.
	state           flushManagerState
	isFlushing      tally.Gauge
	isSnapshotting  tally.Gauge
	isIndexFlushing tally.Gauge
}

func newFlushManager(database database, scope tally.Scope) databaseFlushManager {
	opts := database.Options()
	return &flushManager{
		database:        database,
		opts:            opts,
		pm:              opts.PersistManager(),
		isFlushing:      scope.Gauge("flush"),
		isSnapshotting:  scope.Gauge("snapshot"),
		isIndexFlushing: scope.Gauge("index-flush"),
	}
}

func (m *flushManager) Flush(
	tickStart time.Time,
	dbBootstrapStateAtTickStart DatabaseBootstrapState,
) error {
	// ensure only a single flush is happening at a time
	m.Lock()
	if m.state != flushManagerIdle {
		m.Unlock()
		return errFlushOperationsInProgress
	}
	m.state = flushManagerNotIdle
	m.Unlock()

	defer m.setState(flushManagerIdle)

	// create flush-er
	flush, err := m.pm.StartDataPersist()
	if err != nil {
		return err
	}

	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	m.setState(flushManagerFlushInProgress)
	for _, ns := range namespaces {
		// Flush first because we will only snapshot if there are no outstanding flushes
		flushTimes := m.namespaceFlushTimes(ns, tickStart)
		shardBootstrapTimes, ok := dbBootstrapStateAtTickStart.NamespaceBootstrapStates[ns.ID().String()]
		if !ok {
			// Could happen if namespaces are added / removed.
			multiErr = multiErr.Add(fmt.Errorf(
				"tried to flush ns: %s, but did not have shard bootstrap times", ns.ID().String()))
			continue
		}
		multiErr = multiErr.Add(m.flushNamespaceWithTimes(ns, shardBootstrapTimes, flushTimes, flush))
	}

	// Perform two separate loops through all the namespaces so that we can emit better
	// gauges I.E all the flushing for all the namespaces happens at once and then all
	// the snapshotting for all the namespaces happens at once. This is also slightly
	// better semantically because flushing should take priority over snapshotting.
	m.setState(flushManagerSnapshotInProgress)
	for _, ns := range namespaces {
		var (
			blockSize          = ns.Options().RetentionOptions().BlockSize()
			snapshotBlockStart = m.snapshotBlockStart(ns, tickStart)
			prevBlockStart     = snapshotBlockStart.Add(-blockSize)
		)

		// Only perform snapshots if the previous block (I.E the block directly before
		// the block that we would snapshot) has been flushed.
		if !ns.NeedsFlush(prevBlockStart, prevBlockStart) {
			if err := ns.Snapshot(snapshotBlockStart, tickStart, flush); err != nil {
				detailedErr := fmt.Errorf("namespace %s failed to snapshot data: %v",
					ns.ID().String(), err)
				multiErr = multiErr.Add(detailedErr)
			}
		}
	}

	// mark data flush finished
	multiErr = multiErr.Add(flush.DoneData())

	// flush index data
	// create index-flusher
	indexFlush, err := m.pm.StartIndexPersist()
	if err != nil {
		multiErr = multiErr.Add(err)
		return multiErr.FinalError()
	}

	m.setState(flushManagerIndexFlushInProgress)
	for _, ns := range namespaces {
		var (
			indexOpts    = ns.Options().IndexOptions()
			indexEnabled = indexOpts.Enabled()
		)
		if !indexEnabled {
			continue
		}
		multiErr = multiErr.Add(ns.FlushIndex(tickStart, indexFlush))
	}
	// mark index flush finished
	multiErr = multiErr.Add(indexFlush.DoneIndex())

	return multiErr.FinalError()
}

func (m *flushManager) Report() {
	m.RLock()
	state := m.state
	m.RUnlock()

	if state == flushManagerFlushInProgress {
		m.isFlushing.Update(1)
	} else {
		m.isFlushing.Update(0)
	}

	if state == flushManagerSnapshotInProgress {
		m.isSnapshotting.Update(1)
	} else {
		m.isSnapshotting.Update(0)
	}

	if state == flushManagerIndexFlushInProgress {
		m.isIndexFlushing.Update(1)
	} else {
		m.isIndexFlushing.Update(0)
	}
}

func (m *flushManager) setState(state flushManagerState) {
	m.Lock()
	m.state = state
	m.Unlock()
}

func (m *flushManager) snapshotBlockStart(ns databaseNamespace, curr time.Time) time.Time {
	var (
		rOpts      = ns.Options().RetentionOptions()
		blockSize  = rOpts.BlockSize()
		bufferPast = rOpts.BufferPast()
	)
	// Only begin snapshotting a new block once the previous one is immutable. I.E if we have
	// a 2-hour blocksize, and bufferPast is 10 minutes and our blocks are aligned on even hours,
	// then at:
	// 		1) 1:30PM we want to snapshot with a 12PM block start and 1:30.Add(-10min).Truncate(2hours) = 12PM
	// 		2) 1:59PM we want to snapshot with a 12PM block start and 1:59.Add(-10min).Truncate(2hours) = 12PM
	// 		3) 2:09PM we want to snapshot with a 12PM block start (because the 12PM block can still be receiving
	// 		   "buffer past" writes) and 2:09.Add(-10min).Truncate(2hours) = 12PM
	// 		4) 2:10PM we want to snapshot with a 2PM block start (because the 12PM block can no long receive
	// 		   "buffer past" writes) and 2:10.Add(-10min).Truncate(2hours) = 2PM
	return curr.Add(-bufferPast).Truncate(blockSize)
}

func (m *flushManager) flushRange(ropts retention.Options, t time.Time) (time.Time, time.Time) {
	return retention.FlushTimeStart(ropts, t), retention.FlushTimeEnd(ropts, t)
}

func (m *flushManager) namespaceFlushTimes(ns databaseNamespace, curr time.Time) []time.Time {
	var (
		rOpts            = ns.Options().RetentionOptions()
		blockSize        = rOpts.BlockSize()
		earliest, latest = m.flushRange(rOpts, curr)
	)

	candidateTimes := timesInRange(earliest, latest, blockSize)
	return filterTimes(candidateTimes, func(t time.Time) bool {
		return ns.NeedsFlush(t, t)
	})
}

// flushWithTime flushes in-memory data for a given namespace, at a given
// time, returning any error encountered during flushing
func (m *flushManager) flushNamespaceWithTimes(
	ns databaseNamespace,
	ShardBootstrapStates ShardBootstrapStates,
	times []time.Time,
	flush persist.DataFlush,
) error {
	multiErr := xerrors.NewMultiError()
	for _, t := range times {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := ns.Flush(t, ShardBootstrapStates, flush); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v",
				ns.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}
