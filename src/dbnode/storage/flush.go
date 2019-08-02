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

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/pborman/uuid"
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
	flushManagerColdFlushInProgress
	flushManagerSnapshotInProgress
	flushManagerIndexFlushInProgress
)

type flushManager struct {
	sync.RWMutex

	database  database
	commitlog commitlog.CommitLog
	opts      Options
	pm        persist.Manager
	// state is used to protect the flush manager against concurrent use,
	// while flushInProgress and snapshotInProgress are more granular and
	// are used for emitting granular gauges.
	state           flushManagerState
	isFlushing      tally.Gauge
	isColdFlushing  tally.Gauge
	isSnapshotting  tally.Gauge
	isIndexFlushing tally.Gauge
	// This is a "debug" metric for making sure that the snapshotting process
	// is not overly aggressive.
	maxBlocksSnapshottedByNamespace tally.Gauge

	lastSuccessfulSnapshotStartTime time.Time
}

func newFlushManager(
	database database, commitlog commitlog.CommitLog, scope tally.Scope) databaseFlushManager {
	opts := database.Options()
	return &flushManager{
		database:                        database,
		commitlog:                       commitlog,
		opts:                            opts,
		pm:                              opts.PersistManager(),
		isFlushing:                      scope.Gauge("flush"),
		isColdFlushing:                  scope.Gauge("cold-flush"),
		isSnapshotting:                  scope.Gauge("snapshot"),
		isIndexFlushing:                 scope.Gauge("index-flush"),
		maxBlocksSnapshottedByNamespace: scope.Gauge("max-blocks-snapshotted-by-namespace"),
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

	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	// Perform three separate loops through all the namespaces so that we can
	// emit better gauges, i.e. all the flushing for all the namespaces happens
	// at once, then all the cold flushes, then all the snapshotting. This is
	// also slightly better semantically because flushing should take priority
	// over cold flushes and snapshotting.
	//
	// In addition, we need to make sure that for any given shard/blockStart
	// combination, we attempt a flush and then a cold flush before a snapshot
	// as the snapshotting process will attempt to snapshot any unflushed blocks
	// which would be wasteful if the block is already flushable.
	multiErr := xerrors.NewMultiError()
	if err = m.dataWarmFlush(namespaces, tickStart, dbBootstrapStateAtTickStart); err != nil {
		multiErr = multiErr.Add(err)
	}

	rotatedCommitlogID, err := m.commitlog.RotateLogs()
	if err == nil {
		if err = m.dataColdFlush(namespaces); err != nil {
			multiErr = multiErr.Add(err)
			// If cold flush fails, we can't proceed to snapshotting because
			// commit log cleanup logic uses the presence of a successful
			// snapshot checkpoint file to determine which commit log files are
			// safe to delete. Therefore if a cold flush fails and a snapshot
			// succeeds, the writes from the failed cold flush might be lost
			// when commit logs get cleaned up, leaving the node in an undurable
			// state such that if it restarted, it would not be able to recover
			// the cold writes from its commit log.
			return multiErr.FinalError()
		}

		if err = m.dataSnapshot(namespaces, tickStart, rotatedCommitlogID); err != nil {
			multiErr = multiErr.Add(err)
		}
	} else {
		multiErr = multiErr.Add(fmt.Errorf("error rotating commitlog in mediator tick: %v", err))
	}

	if err = m.indexFlush(namespaces); err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (m *flushManager) dataWarmFlush(
	namespaces []databaseNamespace,
	tickStart time.Time,
	dbBootstrapStateAtTickStart DatabaseBootstrapState,
) error {
	flushPersist, err := m.pm.StartFlushPersist()
	if err != nil {
		return err
	}

	m.setState(flushManagerFlushInProgress)
	multiErr := xerrors.NewMultiError()
	for _, ns := range namespaces {
		// Flush first because we will only snapshot if there are no outstanding flushes
		flushTimes, err := m.namespaceFlushTimes(ns, tickStart)
		if err != nil {
			multiErr = multiErr.Add(fmt.Errorf(
				"error determining namespace flush times for ns: %s, err: %v", ns.ID().String(), err))
			continue
		}
		shardBootstrapTimes, ok := dbBootstrapStateAtTickStart.NamespaceBootstrapStates[ns.ID().String()]
		if !ok {
			// Could happen if namespaces are added / removed.
			multiErr = multiErr.Add(fmt.Errorf(
				"tried to flush ns: %s, but did not have shard bootstrap times", ns.ID().String()))
			continue
		}

		err = m.flushNamespaceWithTimes(
			ns, shardBootstrapTimes, flushTimes, flushPersist)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err = flushPersist.DoneFlush()
	if err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (m *flushManager) dataColdFlush(
	namespaces []databaseNamespace,
) error {
	flushPersist, err := m.pm.StartFlushPersist()
	if err != nil {
		return err
	}

	m.setState(flushManagerColdFlushInProgress)
	multiErr := xerrors.NewMultiError()
	for _, ns := range namespaces {
		if err = ns.ColdFlush(flushPersist); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err = flushPersist.DoneFlush()
	if err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (m *flushManager) dataSnapshot(
	namespaces []databaseNamespace,
	tickStart time.Time,
	rotatedCommitlogID persist.CommitLogFile,
) error {
	snapshotID := uuid.NewUUID()

	snapshotPersist, err := m.pm.StartSnapshotPersist(snapshotID)
	if err != nil {
		return err
	}

	m.setState(flushManagerSnapshotInProgress)
	var (
		maxBlocksSnapshottedByNamespace = 0
		multiErr                        = xerrors.NewMultiError()
	)
	for _, ns := range namespaces {
		snapshotBlockStarts, err := m.namespaceSnapshotTimes(ns, tickStart)
		if err != nil {
			detailedErr := fmt.Errorf(
				"namespace %s failed to determine snapshot times: %v",
				ns.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
			continue
		}

		if len(snapshotBlockStarts) > maxBlocksSnapshottedByNamespace {
			maxBlocksSnapshottedByNamespace = len(snapshotBlockStarts)
		}
		for _, snapshotBlockStart := range snapshotBlockStarts {
			err := ns.Snapshot(
				snapshotBlockStart, tickStart, snapshotPersist)

			if err != nil {
				detailedErr := fmt.Errorf(
					"namespace %s failed to snapshot data for blockStart %s: %v",
					ns.ID().String(), snapshotBlockStart.String(), err)
				multiErr = multiErr.Add(detailedErr)
				continue
			}
		}
	}
	m.maxBlocksSnapshottedByNamespace.Update(float64(maxBlocksSnapshottedByNamespace))

	err = snapshotPersist.DoneSnapshot(snapshotID, rotatedCommitlogID)
	multiErr = multiErr.Add(err)

	finalErr := multiErr.FinalError()
	if finalErr == nil {
		m.lastSuccessfulSnapshotStartTime = tickStart
	}
	return finalErr
}

func (m *flushManager) indexFlush(
	namespaces []databaseNamespace,
) error {
	indexFlush, err := m.pm.StartIndexPersist()
	if err != nil {
		return err
	}

	m.setState(flushManagerIndexFlushInProgress)
	multiErr := xerrors.NewMultiError()
	for _, ns := range namespaces {
		var (
			indexOpts    = ns.Options().IndexOptions()
			indexEnabled = indexOpts.Enabled()
		)
		if !indexEnabled {
			continue
		}
		multiErr = multiErr.Add(ns.FlushIndex(indexFlush))
	}
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

	if state == flushManagerColdFlushInProgress {
		m.isColdFlushing.Update(1)
	} else {
		m.isColdFlushing.Update(0)
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

func (m *flushManager) flushRange(rOpts retention.Options, t time.Time) (time.Time, time.Time) {
	return retention.FlushTimeStart(rOpts, t), retention.FlushTimeEnd(rOpts, t)
}

func (m *flushManager) namespaceFlushTimes(ns databaseNamespace, curr time.Time) ([]time.Time, error) {
	var (
		rOpts            = ns.Options().RetentionOptions()
		blockSize        = rOpts.BlockSize()
		earliest, latest = m.flushRange(rOpts, curr)
	)

	candidateTimes := timesInRange(earliest, latest, blockSize)
	var loopErr error
	return filterTimes(candidateTimes, func(t time.Time) bool {
		needsFlush, err := ns.NeedsFlush(t, t)
		if err != nil {
			loopErr = err
			return false
		}
		return needsFlush
	}), loopErr
}

func (m *flushManager) namespaceSnapshotTimes(ns databaseNamespace, curr time.Time) ([]time.Time, error) {
	var (
		rOpts     = ns.Options().RetentionOptions()
		blockSize = rOpts.BlockSize()
		// Earliest possible snapshottable block is the earliest possible flushable
		// blockStart which is the first block in the retention period.
		earliest = retention.FlushTimeStart(rOpts, curr)
		// Latest possible snapshotting block is either the current block OR the
		// next block if the current time and bufferFuture configuration would
		// allow writes to be written into the next block. Note that "current time"
		// here is defined as "tick start time" because all the guarantees about
		// snapshotting are based around the tick start time, now the current time.
		latest = curr.Add(rOpts.BufferFuture()).Truncate(blockSize)
	)

	candidateTimes := timesInRange(earliest, latest, blockSize)
	var loopErr error
	return filterTimes(candidateTimes, func(t time.Time) bool {
		// Snapshot anything that is unflushed.
		needsFlush, err := ns.NeedsFlush(t, t)
		if err != nil {
			loopErr = err
			return false
		}
		return needsFlush
	}), loopErr
}

// flushWithTime flushes in-memory data for a given namespace, at a given
// time, returning any error encountered during flushing
func (m *flushManager) flushNamespaceWithTimes(
	ns databaseNamespace,
	ShardBootstrapStates ShardBootstrapStates,
	times []time.Time,
	flushPreparer persist.FlushPreparer,
) error {
	multiErr := xerrors.NewMultiError()
	for _, t := range times {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := ns.WarmFlush(t, ShardBootstrapStates, flushPreparer); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v",
				ns.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

func (m *flushManager) LastSuccessfulSnapshotStartTime() (time.Time, bool) {
	return m.lastSuccessfulSnapshotStartTime, !m.lastSuccessfulSnapshotStartTime.IsZero()
}
