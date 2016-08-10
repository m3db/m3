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
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/time"
)

var (
	// errDatabaseAlreadyOpen raised when trying to open a database that is already open
	errDatabaseAlreadyOpen = errors.New("database is already open")

	// errDatabaseNotOpen raised when trying to close a database that is not open
	errDatabaseNotOpen = errors.New("database is not open")

	// errDatabaseAlreadyClosed raised when trying to open a database that is already closed
	errDatabaseAlreadyClosed = errors.New("database is already closed")

	// errCommitLogStrategyUnknown raised when trying to use an unknown commit log strategy
	errCommitLogStrategyUnknown = errors.New("database commit log strategy is unknown")
)

const (
	dbOngoingTasks = 1
)

type databaseState int

const (
	databaseNotOpen databaseState = iota
	databaseOpen
	databaseClosed
)

// database is the internal database interface.
type database interface {
	Database

	// getOwnedShards returns the shards this database owns.
	getOwnedShards() []databaseShard

	// flush flushes in-memory data given a start time.
	flush(t time.Time, async bool)
}

// uniqueIndex provides a unique index for new series
type uniqueIndex interface {
	nextUniqueIndex() uint64
}

// writeCommitLogFn is a method for writing to the commit log
type writeCommitLogFn func(
	series commitlog.CommitLogSeries,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

type db struct {
	sync.RWMutex
	opts             Options
	nowFn            clock.NowFn
	shardScheme      sharding.ShardScheme
	shardSet         sharding.ShardSet
	commitLog        commitlog.CommitLog
	writeCommitLogFn writeCommitLogFn
	state            databaseState
	bsm              databaseBootstrapManager
	fm               databaseFlushManager

	// Contains an entry to all shards for fast shard lookup, an
	// entry will be nil when this shard does not belong to current database
	shards []databaseShard

	created      uint64
	tickDeadline time.Duration

	doneCh chan struct{}
}

// NewDatabase creates a new database
func NewDatabase(shardSet sharding.ShardSet, opts Options) (Database, error) {
	shardScheme := shardSet.Scheme()
	d := &db{
		opts:         opts,
		shardScheme:  shardScheme,
		shardSet:     shardSet,
		shards:       make([]databaseShard, len(shardScheme.All().Shards())),
		nowFn:        opts.GetClockOptions().GetNowFn(),
		tickDeadline: opts.GetRetentionOptions().GetBufferDrain(),
		doneCh:       make(chan struct{}, dbOngoingTasks),
	}
	d.bsm = newBootstrapManager(d)
	d.fm = newFlushManager(d)

	d.commitLog = commitlog.NewCommitLog(opts.GetCommitLogOptions())
	if err := d.commitLog.Open(); err != nil {
		return nil, err
	}

	// TODO(r): instead of binding the method here simply bind the method
	// in the commit log itself and just call "Write()" always
	switch opts.GetCommitLogOptions().GetStrategy() {
	case commitlog.CommitLogStrategyWriteWait:
		d.writeCommitLogFn = d.commitLog.Write
	case commitlog.CommitLogStrategyWriteBehind:
		d.writeCommitLogFn = d.commitLog.WriteBehind
	default:
		return nil, errCommitLogStrategyUnknown
	}

	return d, nil
}

func (d *db) Options() Options {
	return d.opts
}

func (d *db) Open() error {
	d.Lock()
	defer d.Unlock()
	if d.state != databaseNotOpen {
		return errDatabaseAlreadyOpen
	}
	d.state = databaseOpen

	// Initialize shards
	for _, x := range d.shardSet.Shards() {
		d.shards[x] = newDatabaseShard(x, d, d.writeCommitLogFn, d.opts)
	}

	// All goroutines must be accounted for with dbOngoingTasks to receive done signal
	go d.ongoingTick()
	return nil
}

func (d *db) Close() error {
	d.Lock()
	defer d.Unlock()
	if d.state == databaseNotOpen {
		return errDatabaseNotOpen
	}
	if d.state == databaseClosed {
		return errDatabaseAlreadyClosed
	}
	d.state = databaseClosed

	// For now just remove all shards, in future this could be made more explicit.  However
	// this is nice as we do not need to do any other branching now in write/read methods.
	for i := range d.shards {
		d.shards[i] = nil
	}
	for i := 0; i < dbOngoingTasks; i++ {
		d.doneCh <- struct{}{}
	}
	// Finally close the commit log
	return d.commitLog.Close()
}

func (d *db) Write(
	ctx context.Context,
	id string,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	d.RLock()
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID]
	d.RUnlock()

	if shard == nil {
		return fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard.Write(ctx, id, timestamp, value, unit, annotation)
}

func (d *db) ReadEncoded(
	ctx context.Context,
	id string,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	d.RLock()
	if !d.bsm.IsBootstrapped() {
		d.RUnlock()
		return nil, errDatabaseNotBootstrapped
	}
	shardID := d.shardScheme.Shard(id)
	shard := d.shards[shardID]
	d.RUnlock()
	if shard == nil {
		return nil, fmt.Errorf("not responsible for shard %d", shardID)
	}
	return shard.ReadEncoded(ctx, id, start, end)
}

func (d *db) Bootstrap() error {
	return d.bsm.Bootstrap()
}

func (d *db) IsBootstrapped() bool {
	return d.bsm.IsBootstrapped()
}

func (d *db) getOwnedShards() []databaseShard {
	d.RLock()
	// If the database is not open, don't return anything.
	if d.state != databaseOpen {
		d.RUnlock()
		return nil
	}
	shards := d.shardSet.Shards()
	databaseShards := make([]databaseShard, len(shards))
	for i, shard := range shards {
		databaseShards[i] = d.shards[shard]
	}
	d.RUnlock()
	return databaseShards
}

func (d *db) flush(t time.Time, async bool) {
	d.fm.Flush(t, async)
}

func (d *db) ongoingTick() {
	for {
		select {
		case _ = <-d.doneCh:
			return
		default:
			d.splayedTick()
		}
	}
}

func (d *db) splayedTick() {
	shards := d.getOwnedShards()
	if len(shards) == 0 {
		return
	}

	splayApart := d.tickDeadline / time.Duration(len(shards))

	start := d.nowFn()

	var wg sync.WaitGroup
	for i, shard := range shards {
		i := i
		shard := shard
		if i > 0 {
			time.Sleep(splayApart)
		}
		wg.Add(1)
		go func() {
			// TODO(r): instrument timing of this tick
			shard.Tick()
			wg.Done()
		}()
	}

	wg.Wait()

	if d.fm.NeedsFlush(start) {
		d.fm.Flush(start, true)
	}

	end := d.nowFn()
	duration := end.Sub(start)
	// TODO(r): instrument duration of tick
	if duration > d.tickDeadline {
		// TODO(r): log an error and/or increment counter
		_ = "todo"
	} else {
		// throttle to reduce locking overhead during ticking
		time.Sleep(d.tickDeadline - duration)
	}
}

func (d *db) nextUniqueIndex() uint64 {
	created := atomic.AddUint64(&d.created, 1)
	return created - 1
}
