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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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

type flushManagerMetrics struct {
	isFlushing      tally.Gauge
	isSnapshotting  tally.Gauge
	isIndexFlushing tally.Gauge
	// This is a "debug" metric for making sure that the snapshotting process
	// is not overly aggressive.
	maxBlocksSnapshottedByNamespace tally.Gauge
	dataWarmFlushDuration           tally.Timer
	dataAndIndexSnapshotDuration    tally.Timer
	indexFlushDuration              tally.Timer
	commitLogRotationDuration       tally.Timer
}

func newFlushManagerMetrics(scope tally.Scope) flushManagerMetrics {
	return flushManagerMetrics{
		isFlushing:                      scope.Gauge("flush"),
		isSnapshotting:                  scope.Gauge("snapshot"),
		isIndexFlushing:                 scope.Gauge("index-flush"),
		maxBlocksSnapshottedByNamespace: scope.Gauge("max-blocks-snapshotted-by-namespace"),
		dataWarmFlushDuration:           scope.Timer("data-warm-flush-duration"),
		dataAndIndexSnapshotDuration:    scope.Timer("data-and-index-snapshot-duration"),
		indexFlushDuration:              scope.Timer("index-flush-duration"),
		commitLogRotationDuration:       scope.Timer("commit-log-rotation-duration"),
	}
}

type flushManager struct {
	sync.RWMutex

	database  database
	commitlog commitlog.CommitLog
	opts      Options
	pm        persist.Manager
	// state is used to protect the flush manager against concurrent use,
	// while flushInProgress and snapshotInProgress are more granular and
	// are used for emitting granular gauges.
	state   flushManagerState
	metrics flushManagerMetrics

	lastSuccessfulSnapshotStartTime atomic.Int64 // == xtime.UnixNano

	logger               *zap.Logger
	nowFn                clock.NowFn
	readIndexInfoFilesFn fs.ReadIndexInfoFilesFn
}

func newFlushManager(
	database database,
	commitlog commitlog.CommitLog,
	scope tally.Scope,
) databaseFlushManager {
	opts := database.Options()
	return &flushManager{
		database:             database,
		commitlog:            commitlog,
		opts:                 opts,
		pm:                   opts.PersistManager(),
		metrics:              newFlushManagerMetrics(scope),
		logger:               opts.InstrumentOptions().Logger(),
		nowFn:                opts.ClockOptions().NowFn(),
		readIndexInfoFilesFn: fs.ReadIndexInfoFiles,
	}
}

func (m *flushManager) Flush(startTime time.Time) error {
	// ensure only a single flush is happening at a time
	m.Lock()
	if m.state != flushManagerIdle {
		m.Unlock()
		return errFlushOperationsInProgress
	}
	m.state = flushManagerNotIdle
	m.Unlock()

	defer m.setState(flushManagerIdle)

	namespaces, err := m.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	// Perform two separate loops through all the namespaces so that we can
	// emit better gauges, i.e. all the flushing for all the namespaces happens
	// at once then all the snapshotting. This is
	// also slightly better semantically because flushing should take priority
	// over snapshotting.
	//
	// In addition, we need to make sure that for any given shard/blockStart
	// combination, we attempt a flush before a snapshot as the snapshotting process
	// will attempt to snapshot blocks w/ unflushed data which would be wasteful if
	// the block is already flushable.
	multiErr := xerrors.NewMultiError()
	if err = m.dataWarmFlush(namespaces, startTime); err != nil {
		multiErr = multiErr.Add(err)
	}

	start := m.nowFn()
	rotatedCommitlogID, err := m.commitlog.RotateLogs()
	m.metrics.commitLogRotationDuration.Record(m.nowFn().Sub(start))
	rotateCommitlogSuccess := err == nil
	// We want to use the same snapshot ID across all both data/index snapshotting and
	// writing of empty snapshots to disk after a successful index flush.
	snapshotID := uuid.NewUUID()
	if rotateCommitlogSuccess {
		if err = m.dataAndIndexSnapshot(namespaces, startTime, rotatedCommitlogID, snapshotID); err != nil {
			multiErr = multiErr.Add(err)
		}
	} else {
		multiErr = multiErr.Add(fmt.Errorf("error rotating commitlog in mediator tick: %v", err))
	}

	if err = m.indexFlush(namespaces, rotatedCommitlogID, snapshotID); err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (m *flushManager) dataWarmFlush(
	namespaces []databaseNamespace,
	startTime time.Time,
) error {
	flushPersist, err := m.pm.StartFlushPersist()
	if err != nil {
		return err
	}

	m.setState(flushManagerFlushInProgress)
	var (
		start    = m.nowFn()
		multiErr = xerrors.NewMultiError()
	)
	for _, ns := range namespaces {
		// Flush first because we will only snapshot if there are no outstanding flushes.
		flushTimes, err := m.namespaceFlushTimes(ns, startTime)
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		err = m.flushNamespaceWithTimes(ns, flushTimes, flushPersist)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err = flushPersist.DoneFlush()
	if err != nil {
		multiErr = multiErr.Add(err)
	}

	m.metrics.dataWarmFlushDuration.Record(m.nowFn().Sub(start))
	return multiErr.FinalError()
}

func (m *flushManager) dataAndIndexSnapshot(
	namespaces []databaseNamespace,
	startTime time.Time,
	rotatedCommitlogID persist.CommitLogFile,
	snapshotID uuid.UUID,
) error {
	snapshotPersist, err := m.pm.StartSnapshotPersist(snapshotID)
	if err != nil {
		return err
	}
	m.setState(flushManagerSnapshotInProgress)
	var (
		start                           = m.nowFn()
		maxBlocksSnapshottedByNamespace = 0
		multiErr                        = xerrors.NewMultiError()
	)
	for _, ns := range namespaces {
		// NB(bodu): Read in index info files and pass them in for determining
		// whether or not snapshots are warm or cold. We do this here so we're not
		// doing dupe work per block start.
		fsOpts := m.opts.CommitLogOptions().FilesystemOptions()
		infoFiles := m.readIndexInfoFilesFn(
			fsOpts.FilePathPrefix(),
			ns.ID(),
			fsOpts.InfoReaderBufferSize(),
			persist.FileSetFlushType,
		)

		snapshotBlockStarts, err := m.namespaceSnapshotTimes(ns, startTime)
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
				snapshotBlockStart,
				startTime,
				snapshotPersist,
				infoFiles,
			)

			if err != nil {
				detailedErr := fmt.Errorf(
					"namespace %s failed to snapshot data for blockStart %s: %v",
					ns.ID().String(), snapshotBlockStart.String(), err)
				multiErr = multiErr.Add(detailedErr)
				continue
			}
		}
	}
	m.metrics.maxBlocksSnapshottedByNamespace.Update(float64(maxBlocksSnapshottedByNamespace))

	err = snapshotPersist.DoneSnapshot(snapshotID, rotatedCommitlogID)
	multiErr = multiErr.Add(err)

	finalErr := multiErr.FinalError()
	if finalErr == nil {
		m.lastSuccessfulSnapshotStartTime.Store(int64(xtime.ToUnixNano(startTime)))
	}
	m.metrics.dataAndIndexSnapshotDuration.Record(m.nowFn().Sub(start))
	return finalErr
}

func (m *flushManager) indexFlush(
	namespaces []databaseNamespace,
	rotatedCommitlogID persist.CommitLogFile,
	snapshotID uuid.UUID,
) error {
	indexFlush, err := m.pm.StartIndexPersist()
	if err != nil {
		return err
	}

	m.setState(flushManagerIndexFlushInProgress)
	var (
		start    = m.nowFn()
		multiErr = xerrors.NewMultiError()
	)
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

	m.metrics.indexFlushDuration.Record(m.nowFn().Sub(start))
	return multiErr.FinalError()
}

func (m *flushManager) Report() {
	m.RLock()
	state := m.state
	m.RUnlock()

	if state == flushManagerFlushInProgress {
		m.metrics.isFlushing.Update(1)
	} else {
		m.metrics.isFlushing.Update(0)
	}

	if state == flushManagerSnapshotInProgress {
		m.metrics.isSnapshotting.Update(1)
	} else {
		m.metrics.isSnapshotting.Update(0)
	}

	if state == flushManagerIndexFlushInProgress {
		m.metrics.isIndexFlushing.Update(1)
	} else {
		m.metrics.isIndexFlushing.Update(0)
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
		// NB(bodu): Snapshot everything since to account for cold writes/blocks.
		return true
	}), loopErr
}

// flushWithTime flushes in-memory data for a given namespace, at a given
// time, returning any error encountered during flushing
func (m *flushManager) flushNamespaceWithTimes(
	ns databaseNamespace,
	times []time.Time,
	flushPreparer persist.FlushPreparer,
) error {
	multiErr := xerrors.NewMultiError()
	for _, t := range times {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := ns.WarmFlush(t, flushPreparer); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v",
				ns.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

func (m *flushManager) LastSuccessfulSnapshotStartTime() (xtime.UnixNano, bool) {
	snapTime := xtime.UnixNano(m.lastSuccessfulSnapshotStartTime.Load())
	return snapTime, snapTime > 0
}
